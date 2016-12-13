#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import os.path
import stat
import calendar
import socket
import time
import json
import errno
import couchdb
import fuse
import traceback
from decorator import decorator
from fuse import Fuse
from datetime import datetime
from dateutil import parser
from pytz import timezone

import logging
logger = logging.getLogger(__name__)


fuse.fuse_python_api = (0, 2)

ROOT_INODE_ID = "root_inode"
INODE_VIEW_ID = "_design/inode"
INODE_VIEW = {
    "_id": INODE_VIEW_ID,
    "views": {
        "by_path": {
            "map": "function (doc) {\n  if (doc.inode) {\n    emit(doc.inode.path, doc);\n  }\n}"
        }
    },
    "language": "javascript"
}


def _now():
    return datetime.now(timezone('UTC')).isoformat()


def _dt2unixtm(dt):
    return calendar.timegm(dt.utctimetuple())


def _str2dt(s):
    return parser.parse(s)


class CouchStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


@decorator
def trace(f, *args, **kwargs):
    logger.debug('called %s: %s, %s', f.__name__, args, kwargs)
    return f(*args, **kwargs)


@decorator
def exc_handle(f, *args, **kwargs):
    try:
        return f(*args, **kwargs)
    except socket.timeout:
        logger.error('connection timeout: retry %s', f.__name__)
        return f(*args, **kwargs)
    except:
        logger.error('%s failed: %s', f.__name__, traceback.format_exc())
        return -errno.EINVAL


class CouchFS(Fuse):
    def __init__(self, db, *args, **kwargs):
        super(CouchFS, self).__init__(*args, **kwargs)
        self._couchdb = db
        self._inodes = {}  # 使用中の i-Node テーブル
        self._init_root()

    def _get_inode(self, path):
        inode, _ = self._inodes.get(path, (None, 0))
        if inode:
            return inode
        results = self._couchdb.view(INODE_VIEW_ID + '/_view/by_path', include_docs=True)[path]
        if len(results) == 0:
            return
        return results.rows[0].doc

    def _create_inode(self, path, mode, _id=None):
        now = _now()
        inode = {
            "inode": {
                "path": path,
                "mode": mode,
                "nlink": 2 if mode & stat.S_IFDIR else 1,
                "ctime": now,
                "mtime": now
            }
        }
        if _id:
            inode["_id"] = _id
        self._couchdb.save(inode)
        return couchdb.Document(inode)

    def _get_dblock(self, inode):
        dblock = self._couchdb.get_attachment(inode, "dblock")
        return dblock.read() if dblock else ""

    def _put_dblock(self, inode, dblock):
        logger.debug('_put_dblock: inode=%s, dblock=%s', inode, dblock[:16])
        self._couchdb.put_attachment(inode, dblock, "dblock", "application/octet-stream")
#        self._update_inode_timestamp(inode)

    def _get_symlink(self, inode):
        link = self._couchdb.get_attachment(inode, "symlink")
        return link.read() if link else ""

    def _put_symlink(self, inode, link):
        self._couchdb.put_attachment(inode, link, "symlink", "application/octet-stream")

    def _get_dentry(self, inode):
        dentry_json = self._couchdb.get_attachment(inode, "dentry.json")
        return json.load(dentry_json)

    def _put_dentry(self, inode, dentry):
        dentry_json = json.dumps(dentry)
        self._couchdb.put_attachment(inode, dentry_json, "dentry.json", "application/json")
        self._update_inode_timestamp(inode)

    def _update_inode_timestamp(self, inode):
        logger.debug('_update_inode_timestamp: %s', inode)
        inode = self._couchdb[inode['_id']]
        inode['inode']['mtime'] = _now()
        self._couchdb.save(inode)
        logger.debug('_update_inode_timestamp: saved: %s', inode)

    def _add_dentry(self, path, inode):
        #
        # 親ディレクトリの i-Node 取得
        #
        dirname, basename = os.path.split(path)
        par_inode = self._get_inode(dirname)
        #
        # ディレクトリエントリ更新
        #
        dentry = self._get_dentry(par_inode)
        dentry[basename] = inode['_id']
        self._put_dentry(par_inode, dentry)
        return par_inode

    def _del_dentry(self, path):
        #
        # 親ディレクトリの i-Node 取得
        #
        dirname, basename = os.path.split(path)
        par_inode = self._get_inode(dirname)
        dentry = self._get_dentry(par_inode)
        del dentry[basename]
        self._put_dentry(par_inode, dentry)
        return par_inode

    def _init_root(self):
        #
        # View インデックス初期化
        #
        try:
            self._couchdb[INODE_VIEW_ID]
        except couchdb.http.ResourceNotFound:
            logger.info('Created inode view')
            self._couchdb.save(INODE_VIEW)
        #
        # Root i-Node が存在しない場合は初期化する
        #
        root_inode = self._get_inode('/')
        if root_inode is None:
            root_inode = self._create_inode('/', stat.S_IFDIR | 0555, ROOT_INODE_ID)
            dentry = {
                ".": ROOT_INODE_ID,
                "..": ROOT_INODE_ID
            }
            self._put_dentry(root_inode, dentry)
            logger.debug('Created root i-Node: %s', root_inode)

    @exc_handle
    def getattr(self, path):
        logger.debug('called getattr(%s)', path)
        inode = self._get_inode(path)
        if inode is None:
            return -errno.ENOENT
        inode_data = inode['inode']
        st = CouchStat()
        st.st_mode = inode_data['mode']
        att = inode.get('_attachments', {})
        if 'dentry.json' in att:
            sz = att['dentry.json']['length']
        elif 'dblock' in att:
            sz = att['dblock']['length']
        elif 'symlink' in att:
            sz = att['symlink']['length']
        else:
            sz = 0
        st.st_size = sz
        st.st_nlink = inode_data['nlink']
        st.st_atime = int(time.time())
        st.st_mtime = _dt2unixtm(_str2dt(inode_data['mtime']))
        st.st_ctime = _dt2unixtm(_str2dt(inode_data['ctime']))
        return st

    @exc_handle
    def readdir(self, path, offset):
        logger.debug('called readdir(%s, %s)', path, offset)
        inode = self._get_inode(path)
        dentry = self._get_dentry(inode)
        for d in dentry.keys():
            yield fuse.Direntry(d.encode('utf-8'))

    @exc_handle
    def mknod(self, path, mode, dev):
        logger.debug('called mknod(%s, %s, %s)', path, mode, dev)
        if self._get_inode(path):
            return -errno.EEXIST

        inode = self._create_inode(path, mode)
        self._add_dentry(path, inode)

    @exc_handle
    def unlink(self, path):
        logger.debug('called unlink(%s)', path)
        #
        # TODO: i-Node の nlink をチェック
        #
        inode = self._get_inode(path)
        if inode is None:
            return -errno.ENOENT

        self._couchdb.delete(inode)
        self._del_dentry(path)

    @exc_handle
    def rmdir(self, path):
        logger.debug('called rmdir(%s)', path)
        inode = self._get_inode(path)
        if inode is None:
            return -errno.ENOENT
        dentry = self._get_dentry(inode)
        if len(dentry) > 2:
            return -errno.ENOTEMPTY

        self._couchdb.delete(inode)
        self._del_dentry(path)

    @exc_handle
    def open(self, path, flags):
        logger.debug('called open(%s, %s)', path, flags)
        inode = self._get_inode(path)
        if inode is None:
            return -errno.ENOENT
        if path not in self._inodes:
            self._inodes[path] = [inode, 0]
        self._inodes[path][1] += 1

    @exc_handle
    def release(self, path, flags):
        logger.debug('called release(%s, %s)', path, flags)
        _, refs = self._inodes[path]
        refs -= 1
        if refs <= 0:
            del self._inodes[path]

    @exc_handle
    def read(self, path, size, offset):
        logger.debug('called read(%s, %s, %s)', path, size, offset)
        inode, _ = self._inodes[path]
        dblock = self._get_dblock(inode)
        return dblock[offset:offset + size]

    @exc_handle
    def write(self, path, buf, offset):
        logger.debug('called write(%s, %s, %s)', path, buf[:16], offset)
        inode, _ = self._inodes[path]
        buf_len = len(buf)
        dblock = self._get_dblock(inode)
        dblock = dblock[:offset] + buf + dblock[offset + buf_len:]
        self._put_dblock(inode, dblock)
        return buf_len

    @exc_handle
    def mkdir(self, path, mode):
        logger.debug('called mkdir(%s, %s)', path, mode)
        if self._get_inode(path):
            return -errno.EEXIST

        inode = self._create_inode(path, stat.S_IFDIR | mode)
        par_inode = self._add_dentry(path, inode)
        dentry = {
            ".": inode['_id'],
            "..": par_inode['_id']
        }
        self._put_dentry(inode, dentry)

    @exc_handle    
    def truncate(self, path, length):
        logger.debug('called truncate(%s, %s)', path, length)
        inode, _ = self._inodes[path]
        dblock = self._get_dblock(inode)
        dblock = dblock[:length]
        self._put_dblock(inode, dblock)

    @exc_handle    
    def readlink(self, path):
        logger.debug('called readlink(%s)', path)
        inode = self._get_inode(path)
        if inode is None:
            return -errno.ENOENT
        return self._get_symlink(inode)

    @exc_handle
    def symlink(self, source, path):
        logger.debug('called symlink(%s, %s)', source, path)
        if self._get_inode(path):
            return -errno.EEXIST
        inode = self._create_inode(path, stat.S_IFLNK | 0777)
        self._put_symlink(inode, source)
        self._add_dentry(path, inode)

    @exc_handle
    def chmod(self, path, mode):
        logger.debug('called chmod(%s, %s)', path, mode)
        inode = self._get_inode(path)
        if inode is None:
            return -errno.ENOENT
        inode['inode']['mode'] = mode
        self._couchdb.save(inode)

    @exc_handle
    def chown(self, path, user, group):
        logger.debug('called chown(%s, %s, %s)', path, user, group)

#    def link(self, path, link_name):
#        logger.debug('called link(%s, %s)', path, link_name)

    def utime(self, path, times):
        logger.debug('called utime(%s, %s)', path, times)


COUCHDB_URL = 'http://localhost:5984/'
COUCHDB_NAME = 'couchfs'

LOG_FORMAT = '[%(asctime)s] %(levelname)s:%(name)s:%(message)s'
LOG_FILEPATH = '/var/log/couchfs.log'

DEFAULT_SESSION_TIMEOUT = 3


def main():
    usage = """
Userspace CouchDB File System

%prog <URL> <mountpoint> [options]
"""
    if len(sys.argv) < 3:
        print usage
        exit(1)
    url = sys.argv[1]

    logging.basicConfig(level='DEBUG', format=LOG_FORMAT, filename=LOG_FILEPATH)
    logger.info('CouchDB File System started')

    couch = couchdb.Server(url, session=couchdb.Session(timeout=DEFAULT_SESSION_TIMEOUT))
    try:
        db = couch[COUCHDB_NAME]
    except couchdb.http.ResourceNotFound:
        db = couch.create(COUCHDB_NAME)
        logger.info('Created DB for couchfs: %s', COUCHDB_NAME)
    server = CouchFS(
        db,
        version="%prog " + fuse.__version__,
        usage=usage,
        dash_s_do='setsingle'
    )
    server.parse(values=server, errex=1)
    server.main()
    logger.info('CouchDB File System finished')

if __name__ == '__main__':
    main()
