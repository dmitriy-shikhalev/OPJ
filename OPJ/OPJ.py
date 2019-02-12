import bisect
import glob
import os
import queue
import struct
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any


MAX_BUFFER_SIZE = 1 * 1024  # 1 KB


class Error(Exception):
    pass


class TryToCreateExistedList(Error):
    pass


class TryToOpenUnexistedList(Error):
    pass


@dataclass(order=True)
class OrderedItem:
    value: Any = field()
    iter: Any = field(compare=False)
    file: Any = field(compare=False)


@dataclass(order=True)
class PriorityItem:
    priority: int
    item: Any = field(compare=False)


class _Journal:
    journals = dict()

    def __init__(self, path: str, name: str, fmt: str):
        self.path = path
        self.name = name
        self.fmt = fmt
        self.lock = threading.Lock()

    @property
    def is_active(self):
        return not self.name.startswith('_')

    @property
    def filename(self):
        return os.path.join(self.path, self.name)

    @property
    def size(self):
        return struct.calcsize(self.fmt)

    def activate(self):
        with self.lock:
            src = self.filename
            self.name = self.name.lstrip('_')
            dst = self.filename

            os.rename(src, dst)

    def deactivate(self):
        with self.lock:
            src = self.filename
            self.name = '_' + self.name
            dst = self.filename

            os.rename(src, dst)

    def __del__(self):
        if not self.is_active:
            with self.lock:
                os.remove(self.filename)


class JournalReader(_Journal):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        with self.lock:
            self._len = os.path.getsize(self.filename) // self.size

    @property
    def filename(self):
        if not os.path.exists(
            super().filename
        ):
            if os.path.exists(
                    os.path.join(self.path, f'_{self.name}')
            ):
                self.name = f'_{self.name}'
        return super().filename

    def select(self, from_item, till_item):
        if from_item is None:
            from_idx = 0
        else:
            from_idx = bisect.bisect_left(self, from_item)
        if till_item is None:
            till_idx = len(self)
        else:
            till_idx = bisect.bisect_right(self, till_item)

        while from_idx < till_idx:
            yield self[from_idx]
            from_idx += 1

    def __enter__(self):
        self.lock.acquire()
        self.fd = open(
                self.filename,
                mode='rb'
        )
        self._len = os.path.getsize(self.filename) // self.size
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()
        self.fd.close()
        del self.fd

    def __getitem__(self, idx: int) -> tuple:
        if idx >= len(self):
            raise IndexError
        self.fd.seek(self.size * idx)
        bs = self.fd.read(self.size)
        return struct.unpack(self.fmt, bs)

    def __len__(self):
        return self._len


class JournalWriter(_Journal):
    def __init__(self, path: str, name: str, fmt: str):
        name = '_' + name
        super().__init__(path, name, fmt)

    def append(self, item):
        self.fd.write(
            struct.pack(self.fmt, *item)
        )

    def __enter__(self):
        self.fd = open(
            self.filename,
            mode='wb'
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fd.close()
        del self.fd


class Buffer:
    filename = 'buffer'

    def __init__(self, path, fmt):
        self.path = path
        self.fmt = fmt
        self._items = []
        self.read_from_disk()

    @property
    def full_fn(self):
        return os.path.join(
            self.path,
            self.filename,
        )

    @property
    def fd(self):
        if not hasattr(self, '_fd'):
            self._fd = open(self.full_fn, 'wb')
        return self._fd

    def append(self, item):
        bisect.insort(self._items, item)
        self.write_on_disk(item)

    def read_from_disk(self):
        size = struct.calcsize(self.fmt)
        try:
            with open(self.full_fn, 'rb') as fd:
                bs = fd.read()
        except FileNotFoundError:
            bs = b''

        if not bs:
            return

        i = 0
        while i < len(bs):
            item = struct.unpack(self.fmt, bs[i:i + size])
            self.append(item)
            i += size

    def write_on_disk(self, item):
        bs = struct.pack(self.fmt, *item)
        self.fd.write(bs)
        self.fd.flush()

    def flush(self, name):
        writer = JournalWriter(self.path, name, self.fmt)
        with writer:
            for item in self._items:
                writer.append(item)
            self._items = []
        writer.activate()

    def select(self, from_, to):
        if from_ is None:
            from_idx = 0
        else:
            from_idx = bisect.bisect_left(self, from_)
        if to is None:
            to_idx = len(self)
        else:
            to_idx = bisect.bisect_right(self, to)

        while from_idx < to_idx:
            yield self[from_idx]
            from_idx += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __len__(self):
        return len(self._items)

    def __getitem__(self, idx):
        return self._items[idx]


class Combine(threading.Thread):
    daemon = True

    def __init__(self, journal, path, fmt):
        super().__init__()
        self.journal = journal
        self.path = path
        self.fmt = fmt

    def run(self):
        while True:
            priority_item1 = self.journal.file_queue.get()
            priority_item2 = self.journal.file_queue.get()

            outname = f'{uuid.uuid4().hex}.opj'

            writer = JournalWriter(self.path, outname, self.fmt)
            with priority_item1.item, priority_item2.item, writer:
                iter1 = (item for item in priority_item1.item)
                iter2 = (item for item in priority_item2.item)
                try:
                    item1 = next(iter1)
                except StopIteration:
                    item1 = None
                try:
                    item2 = next(iter2)
                except StopIteration:
                    item2 = None

                length = 0
                while item1 is not None or item2 is not None:
                    length += 1
                    if item1 is None:
                        writer.append(item2)
                        try:
                            item2 = next(iter2)
                        except StopIteration:
                            item2 = None
                    elif item2 is None:
                        writer.append(item1)
                        try:
                            item1 = next(iter1)
                        except StopIteration:
                            item1 = None
                    elif item1 < item2:
                        writer.append(item1)
                        try:
                            item1 = next(iter1)
                        except StopIteration:
                            item1 = None
                    else:
                        writer.append(item2)
                        try:
                            item2 = next(iter2)
                        except StopIteration:
                            item2 = None

            writer.activate()

            priority_item1.item.deactivate()
            priority_item2.item.deactivate()

            # priority_item1.item.remove()
            # priority_item2.item.remove()

            reader = JournalReader(
                self.path,
                outname,
                self.fmt
            )

            self.journal._file_list.append(reader)

            self.journal._clean_file_list()

            self.journal.file_queue.put(
                PriorityItem(
                    priority=length,
                    item=reader
                )
            )


class OrderedPersistentJournal():
    def __init__(self, path, fmt):
        self.path = path
        self.fmt = fmt
        self.file_queue = queue.PriorityQueue()
        self.storage = dict()
        self.buffer = Buffer(self.path, self.fmt)
        self.buffer.read_from_disk()
        self._file_list = []

        self.combine_thread = Combine(self, self.path, self.fmt)
        self.combine_thread.start()

    @classmethod
    def new(cls, path, fmt):
        if not os.path.exists(path):
            os.mkdir(path)
        listdir = os.listdir(path)
        if listdir:
            raise TryToCreateExistedList(path, format)
        with open(os.path.join(path, 'fmt'), 'w') as fd:
            fd.write(fmt)
        return cls(path, fmt)

    @classmethod
    def open(cls, path):
        if not os.path.exists(path):
            raise TryToOpenUnexistedList(path)
        try:
            fmt = open(os.path.join(path, 'fmt'), 'r').read()
        except FileNotFoundError:
            raise TryToOpenUnexistedList(path)
        for fn in glob.glob(f'{path}/_*.opj'):
            os.remove(fn)
        inst = cls(path, fmt)
        inst._file_list = [
            JournalReader(
                inst.path,
                fn, inst.fmt
            ) for fn in os.listdir(path) if fn.endswith('.opj')
        ]
        return inst

    def append(self, vals):
        # bs = struct.pack(self.fmt, *vals)
        self.buffer.append(vals)
        if len(self.buffer) > MAX_BUFFER_SIZE:
            new_file_name = f'{uuid.uuid4().hex}.opj'
            self.buffer.flush(new_file_name)
            journal_reader = JournalReader(self.path, new_file_name, self.fmt)
            self._file_list.append(
                journal_reader
            )
            self.file_queue.put(
                PriorityItem(
                    priority=len(self.buffer),
                    item=journal_reader
                )
            )

    def _clean_file_list(self):
        self._file_list = [file for file in self._file_list if file.is_active]

    def __contains__(self, vals):
        for _ in self.select(vals, vals):
            return True
        return False

    def select(self, val_from, val_till):
        list_ = self._file_list.copy()
        list_.append(self.buffer)
        q = queue.PriorityQueue()
        for reader in list_:
            iter_ = iter(reader.__enter__().select(val_from, val_till))
            try:
                item = next(iter_)
            except StopIteration:
                pass
            else:
                q.put(
                    OrderedItem(value=item, iter=iter_, file=reader)
                )

        while q.qsize():
            item = q.get()
            yield item.value
            try:
                val = next(item.iter)
            except StopIteration:
                item.file.__exit__(None, None, None)
            else:
                q.put(
                    OrderedItem(value=val, iter=item.iter, file=item.file)
                )

    def __iter__(self):
        file_list = [file.__enter__() for file in self._file_list]
        iters = [iter(file) for file in file_list]
        iters.append(iter(self.buffer))
        item_queue = queue.PriorityQueue()

        i_remove = []
        for i, iter_ in enumerate(iters):
            try:
                value = next(iter_)
            except StopIteration:
                i_remove.append(i)
            else:
                item_queue.put(OrderedItem(
                    value=value,
                    iter=iter_,
                    file=file_list[i] if i < len(file_list) else None,
                ))
        for i in i_remove:
            try:
                file_list[i].__exit__()
                del file_list[i]
            except IndexError:  # Buffer is ended
                pass
        while item_queue.qsize():
            item = item_queue.get()
            yield item.value
            try:
                value = next(item.iter)
            except StopIteration:
                if item.file is not None:
                    item.file.__exit__(0, 0, None)
            else:
                item_queue.put(OrderedItem(
                    value=value,
                    iter=item.iter,
                    file=item.file,
                ))

    def __len__(self):
        return sum(
            map(
                len,
                (
                    self.buffer,
                    *self._file_list,
                )
            )
        )
