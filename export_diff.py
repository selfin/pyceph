#!/usr/bin/python
# -*- coding: utf-8 -*-
BLOCKSIZE = 256 << 10
DIFF_MAGIC = 'rbd diff v1\n'
import struct
from ctypes import *
import os

_libc = CDLL('libc.so.6', use_errno=True)

_fallocate = _libc.fallocate64
_fallocate.argtypes = [c_int, c_int, c_longlong, c_longlong]
_fallocate.restype = c_int
_FALLOC_FL_KEEP_SIZE = 1
_FALLOC_FL_PUNCH_HOLE = 2


def punch(fh, offset, length):
    if hasattr(fh, 'punch'):
        fh.punch(offset, length)
    elif _fallocate(fh.fileno(), _FALLOC_FL_PUNCH_HOLE | _FALLOC_FL_KEEP_SIZE,
                    offset, length):
        raise OSError('fallocate() failed: %s' % os.strerror(get_errno()))


def read_items(fh, fmt):
    size = struct.calcsize(fmt)
    buf = fh.read(size)
    items = struct.unpack(fmt, buf)
    if len(items) == 1:
        return items[0]
    else:
        return items


def apply_diff(ifh, ofh, verbose=True):
    """

    :param ifh: Input file (.diff)
    :type ifh: file
    :param ofh: Output file
    :type ofh: file
    :param verbose: verb
    :type verbose: bool
    :return:
    :rtype:
    """
    total_size = 0
    total_changed = 0
    # Read header
    buf = ifh.read(len(DIFF_MAGIC))
    if buf != DIFF_MAGIC:
        raise IOError('Missing diff magic string')
    # Read each record
    while True:
        type = read_items(ifh, 'c')
        if type in ('f', 't'):
            # Source/dest snapshot name => ignore
            size = read_items(ifh, '<I')
            ifh.read(size)
        elif type == 's':
            # Image size
            total_size = read_items(ifh, '<Q')
            ofh.truncate(total_size)
        elif type == 'w':
            # Data
            offset, length = read_items(ifh, '<QQ')
            total_changed += length
            ofh.seek(offset)
            while length > 0:
                buf = ifh.read(min(length, BLOCKSIZE))
                ofh.write(buf)
                length -= len(buf)
        elif type == 'z':
            # Zero data
            offset, length = read_items(ifh, '<QQ')
            total_changed += length
            punch(ofh, offset, length)
        elif type == 'e':
            if ifh.read(1) != '':
                raise IOError("Expected EOF, didn't find it")
            break
        else:
            raise ValueError('Unknown record type: %s' % type)
    if verbose:
        print '%d bytes written, %d total' % (total_changed, total_size)

import subprocess
from .ceph import logger


def rbd_exec(pool, cmd, verbose=False, *args):
    cmd_exec = ['rbd', cmd] + list(args) + ['-p', pool]
    if verbose:
        logger.debug(' '.join(cmd_exec))
    subprocess.check_call(cmd_exec)


def export_diff(pool, image, snapshot, out_file, basis=None, fh=subprocess.PIPE, config=None):
    cmd = ['rbd', 'export-diff', '--no-progress', '-p', pool, image]
    if snapshot:
        cmd.extend(['--snap', snapshot])
    if config:
        cmd.extend(["-c", config])
    if basis is not None:
        cmd.extend(['--from-snap', basis])
    cmd.extend([out_file])
    logger.debug("Generated command: %s", ' '.join(cmd))
    return subprocess.Popen(cmd, stdout=fh)


def merge_diff(pool, first_diff, second_diff, out_file, basis=None, fh=subprocess.PIPE):
    "rbd merge-diff snap1.diff snap2.diff combined.diff"
    cmd = ['rbd', 'merge-diff', '--no-progress', '-p', pool, first_diff,
            second_diff, out_file]
    if basis is not None:
        cmd.extend(['--from-snap', basis])
    logger.debug("Generated command: %s", ' '.join(cmd))
    return subprocess.Popen(cmd, stdout=fh)


