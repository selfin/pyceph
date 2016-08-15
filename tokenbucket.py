#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division
from time import time


class TokenBucket(object):
    """An implementation of the token bucket algorithm.

    >>> bucket = TokenBucket(80, 0.5)
    >>> print bucket.consume(10)
    True
    >>> print bucket.consume(90)
    False
    """
    def __init__(self, tokens, fill_rate):
        """tokens is the total tokens in the bucket. fill_rate is the
        rate in tokens/second that the bucket will be refilled."""
        self.capacity = float(tokens)
        self._tokens = float(tokens)
        self.fill_rate = float(fill_rate)
        self.timestamp = time()

    def consume(self, tokens):
        """Consume tokens from the bucket. Returns True if there were
        sufficient tokens otherwise False."""
        if tokens <= self.tokens:
            self._tokens -= tokens
        else:
            return False
        return True

    def get_tokens(self):
        now = time()
        if self._tokens < self.capacity:
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
        self.timestamp = now
        return self._tokens
    tokens = property(get_tokens)


if __name__ == '__main__':
    import sys

    def sizeof_fmt(num, suffix='B'):
        for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    def print_progress():
        progress = cur / total * 100

        if not progress == 100:
            sys.stdout.write('\rProgress: %d%%(%s/%s)...' % (progress, sizeof_fmt(cur), sizeof_fmt(total)))
        else:
            sys.stdout.write("\rProgress: %d%%...done.\n" % progress)
        sys.stdout.flush()

    from time import sleep
    bs = 4096
    total = bs * 60
    limit = bs * 4  # 16kbps
    cur = 0
    bucket = TokenBucket(limit, limit)

    f = open("/tmp/test_bucket", 'wb')
    sleeping = time()
    while cur <= total:
        if time() - sleeping >= 10:
            # flush buffer every x seconds
            # or can rely on os functions
            sleeping = time()
            f.flush()
        if bucket.consume(bs):
            # actual bs
            f.write(b"0" * bs)
            print_progress()
            cur += bs
        else:
            sleep(1)
    f.close()

