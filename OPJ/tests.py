import gc
import os
import random
import struct
import time
from queue import Queue

import pytest
from unittest import mock

from . import OPJ


def test_journal_write_read(tmpdir):
    with OPJ.JournalWriter(
        tmpdir.strpath,
        'tmpfile',
        'iiLf'
    ) as write_journal:
        for item in (
            (2, 3, 5, -0.1),
            (300, 4000, 0, 100.222)
        ):
            write_journal.append(item)

    with OPJ.JournalReader(
        tmpdir.strpath,
        '_' + 'tmpfile',
        'iiLf'
    ) as read_journal:
        assert len(read_journal) == 2
        assert read_journal[0][:3] == (2, 3, 5)
        assert read_journal[0][3] - (-0.1) < 1E-6
        assert read_journal[1][:3] == (300, 4000, 0)
        assert read_journal[1][3] - 100.222 < 1E-6


def test_buffer(tmpdir):
    count = 10
    fmt = 'If'
    dirname = tmpdir.strpath
    buffer = OPJ.Buffer(dirname, fmt)
    items = []

    for _ in range(count):
        item = (
            random.randint(0, 1_000_000),
            random.random()
        )
        items.append(item)
        buffer.append(item)

    for i in range(1, len(buffer)):
        assert buffer[i - 1] <= buffer[i]

    ls = list(buffer)
    assert len(ls) == count

    bs = b''
    for item in items:
        bs += struct.pack(fmt, *item)
    assert open(os.path.join(
        dirname,
        'buffer'
    ), 'rb').read() == bs

    del buffer
    buffer = OPJ.Buffer(dirname, fmt)
    assert all(
        [
            i1 == i2 and abs(f1 - f2) < 1E-6
            for (i1, f1), (i2, f2) in zip(list(buffer), ls)
        ]
    )


def test_combine(tmpdir):
    count = 100
    fmt = 'i'
    q = Queue()
    path = tmpdir.strpath
    name1 = 'test1'
    name2 = 'test2'

    with OPJ.JournalWriter(path, name1, fmt) as writer1,\
            OPJ.JournalWriter(path, name2, fmt) as writer2:
        for i in range(count // 2):
            writer1.append((i,))
            writer2.append((i*2,))
        for i in range(count // 2, count):
            writer1.append((i * 10,))
            writer2.append((i * 2,))

        writer1.activate()
        writer2.activate()

    q.put(OPJ.PriorityItem(
        priority=count,
        item=OPJ.JournalReader(path, name1, fmt))
    )
    q.put(OPJ.PriorityItem(
        priority=count,
        item=OPJ.JournalReader(path, name2, fmt))
    )

    m = mock.Mock()
    m.file_queue = q

    combine = OPJ.Combine(m, tmpdir.strpath, fmt)
    combine.start()

    time.sleep(0.5)

    gc.collect()

    assert len([
        fn for fn in os.listdir(tmpdir.strpath)
        if not fn.startswith('_')]) == 1

    l = []
    with OPJ.JournalReader(
            path,
            os.listdir(tmpdir.strpath)[0],
            fmt
    ) as reader:
        for line in reader:
            l.append(line[0])

    assert l == sorted(l)


def test_ordered_persistent_journal(tmpdir):
    ordered_persisternt_jounal = OPJ.OrderedPersistentJournal.new(
        path=tmpdir.strpath,
        fmt='i'
    )
    for _ in range(10_000):
        ordered_persisternt_jounal.append((random.randint(-1_000, 1_000),))

    gc.collect()

    time.sleep(1)

    assert len([
        fn for fn in os.listdir(tmpdir.strpath)
        if not fn.startswith('_')
            and fn not in ('fmt', 'buffer')]) == 1

    prev = None
    for i in ordered_persisternt_jounal:
        if prev is not None:
            assert i >= prev
        prev = i

    ordered_persisternt_jounal = OPJ.OrderedPersistentJournal.open(
        path=tmpdir.strpath,
    )
    prev = None
    for i in ordered_persisternt_jounal:
        if prev is not None:
            assert i >= prev
        prev = i


def test_ordered_persistent_journal_2(tmpdir):
    ordered_persisternt_jounal = OPJ.OrderedPersistentJournal.new(
        path=tmpdir.strpath,
        fmt='dI'
    )
    l = []
    for i in range(100_000):
        item = (random.random(), random.randint(0, 10**9))
        ordered_persisternt_jounal.append(item)
        l.append(item)

    assert set(l) == set(sorted(l))

    l.sort()

    l2 = list(ordered_persisternt_jounal)

    assert len(l) == len(l2)
    assert l == l2

    # gc.collect()
    time.sleep(2)
    # assert len([
    #     fn
    #     for fn in os.listdir(tmpdir.strpath)
    #     if fn.startswith('_')
    # ]) == 0
    assert len([
        fn
        for fn in os.listdir(tmpdir.strpath)
        if fn not in ('fmt', 'buffer') and not fn.startswith('_')
    ]) == 1
