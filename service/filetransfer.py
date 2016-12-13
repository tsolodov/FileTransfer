from nodeset.core import node, utils
from nodeset.core.config import Configurator
from nodeset.common import log

from nodeset.common.twistedapi import runApp, NodeSetAppOptions

from twisted.application import service
from twisted.python import usage
from twisted.protocols.basic  import FileSender
from  twisted.internet.abstract import FileDescriptor
import os, logging
from twisted.internet.fdesc import writeToFD, setNonBlocking
from twisted.internet import  defer
from twisted.python.failure import Failure
from ftrecback.messages import  fileTransferReq
from twisted.python.filepath import FilePath
from twisted.internet import reactor
from time import time


class FileTransferOptions(NodeSetAppOptions):
    
    optParameters = [['config', None, '/etc/ft/rb.conf', 'Configuration file'],
                     ]
    optFlags = [['passive',None,"passive transport. it does not sent request resume-file-transfer-req."],]



class dstFile(FileDescriptor):
    
    def __init__(self, dst):
        FileDescriptor.__init__(self)
        self.connected = 1
        self.disconnected = 0
        try:
            self.file = os.open(dst, os.O_WRONLY|os.O_CREAT)
        except Exception, e:
            log.msg(str(e), logLevel=logging.ERROR)
#            return Failure(e)   

        
    def writeSomeData(self, data):
        return writeToFD(self.file, data)
    
    def closeFile(self, ign):
        try:
            os.close(self.file)
            return defer.succeed(None)
        except Exception, e:
            return defer.fail(str(e))
            
        
    def fileno(self):
        return self.file
        



        
class FileTransferNode(node.Node):
    def __init__(self,conf, passive=False):
        node.Node.__init__(self, name='file-transfer')
        self.passive = passive
        self.conf = conf
        self.working_queue={}
        self.queue = defer.DeferredQueue()
        self.working=0
        self.QUEUESIZE=int(self.conf.get('transport', 'threads'))
        self.PENDINGLIMIT=100
        self.timestamp=time()
        self.stopped=False
        if not self.passive:
            self._sched_resume_ft()
        self._sched_stop_ft()
        self._requeue()


    def _requeue(self):
        self._check_queue()
        try:
            if self.stopped and len(self.queue.pending)>0 and self.working < self.QUEUESIZE:
                log.msg("Requeue files from queue")
    
                for i in range(self.QUEUESIZE):
                    self.queue.get().addCallback(self.startCopying)
        except:
            pass
         
        reactor.callLater(5, self._requeue)
        


    def _check_queue(self):
        log.msg("Starting Mr. Proper for working queue.", logLevel=logging.DEBUG)
        for k in self.working_queue.keys():
            try:
                if time() - self.working_queue[k]['time'] > 300:
                    msg = self.working_queue[k]['msg']
                    log.msg("Requeue job %s -> %s by timeout." % (msg.src.getValue(), msg.dst.getValue()),logLevel=logging.ERROR)
                    self.working_queue.pop(k, 0)
                    self.startCopying(msg)
                    self.working=self.working-1
            except KeyError:
                log.msg("Key %s already removed. Ignoring." % k, logLevel=logging.DEBUG)

            


    def _sched_resume_ft(self):
        if len(self.queue.pending) == 0 and (time()-self.timestamp) > 60:
            self.resumefiletransferreq()        
            log.msg("Files in queue: %s, sending 'resume-file-transfer-req'. Queue size: %s. Pending: %s." % (self.working,self.QUEUESIZE,len(self.queue.pending)))
        reactor.callLater(10, self._sched_resume_ft)
        
        
        
    def _sched_stop_ft(self):
        if len(self.queue.pending) > 0:
            self.stopfiletransferreq()
#             if len(self.queue.pending) > 0:
#                 self.queue.get().addCallback(self.startCopying)        
            log.msg("Files in queue: %s. Queue size: %s. Pending: %s." % (self.working,self.QUEUESIZE,len(self.queue.pending)))
        reactor.callLater(10, self._sched_stop_ft)        

        
    def stopfiletransferreq(self):
        self.stopped=True
        #log.msg("Stopping ft", logLevel=logging.DEBUG)
#        self.publish('stop-file-transfer-req',fileTransferReq)


    def resumefiletransferreq(self):
        self.stopped=False
        if not self.passive:
            log.msg("Requesting files for backup.")
            self.publish('resume-file-transfer-req',fileTransferReq)
        
        
    def controlQueue(self):
#TODO: review this logic.
        log.msg("Queue stats: working: %s. Queue size: %s. Pending: %s." % (self.working,self.QUEUESIZE, len(self.queue.pending)))
        if self.working >= self.QUEUESIZE and len(self.queue.pending) > self.PENDINGLIMIT:
            self.stopfiletransferreq()
        
        elif self.stopped and len(self.queue.pending) <self.PENDINGLIMIT and not self.passive and (time()-self.timestamp) > 60:
            self.resumefiletransferreq()
            
        elif self.working < self.QUEUESIZE and len(self.queue.pending) > 0:
            pass
            #log.msg("Re-queue files")
            #self.queue.get().addCallback(self.startCopying)
#         elif self.working == 0 and len(self.queue.pending)==0:
#             log.msg("Svobodnaya kassa!!!")
#             self.resumefiletransferreq()
            
        else:
            log.msg("Queue stats: %s %s %s %s" % (self.stopped,self.working,self.QUEUESIZE, len(self.queue.pending) ))
        
    def incpendingqueue(self, ign=None, msg=None):
        self.working=self.working+1
        self.working_queue[msg.src.getValue()]={}
        self.working_queue[msg.src.getValue()]['time']=time()
        self.working_queue[msg.src.getValue()]['msg']=msg
        
        log.msg("Files processing %d" % self.working,logLevel=logging.DEBUG)
        self.controlQueue()
#        return defer.succeed(self.working)

    def decpendingqueue(self, ign=None, msg=None):
        self.working=self.working-1
        self.working_queue.pop(msg.src.getValue(), 0)
        log.msg("Files processing %d" % self.working,logLevel=logging.DEBUG)
#        if self.working < self.QUEUESIZE and len(self.queue.pending) > 0 and not (str(ign) == 'SAME_SIZE'):
        if self.working < self.QUEUESIZE and len(self.queue.pending) > 0:
            
            log.msg("Scheduling next backup",logLevel=logging.DEBUG)
            self.queue.get().addCallback(self.startCopying)
            
        self.controlQueue()
        return defer.succeed(self.working)            
        
    def do_subscribe(self):
        self.subscribe('file-transfer-req')


    def onEvent(self, event, msg):
                  
        if event == 'file-transfer-req':
            log.msg("Got file-transfer-req. ", system=self,logLevel=logging.DEBUG)
            self.timestamp=time()
            self.startCopying(msg)
            
            
     
            
    def startCopying(self, msg):
        
        def _done(ign, src, dst, f):
            log.msg("File copied successfully %(src)s -> %(dst)s" % {'src': src, 
                                                                     'dst': dst}
                                                                    )
            self.publish('file-transfer-ack', fileTransferReq, id=msg.id.getValue(), state=0, table=msg.table.getValue())
        
            
        def _err_done(reason, src, dst, sendreq=None):
            log.msg("File transfer error %(src)s -> %(dst)s. Reason: %(reason)s" % {'src': src, 
                                                                           'dst': dst, 
                                                                           'reason': reason.getErrorMessage()}, 
                                                                           logLevel=logging.ERROR)
            if sendreq:
                self.publish('file-transfer-ack', fileTransferReq, id=msg.id.getValue(), state=1, table=msg.table.getValue())
            return defer.fail(reason)

        
        if self.working>self.QUEUESIZE:
            self.queue.put(msg)
            
            return

        
        self.incpendingqueue(None, msg=msg)


        srcfile=msg.src.getValue()
        dstfile=msg.dst.getValue()
        s=FilePath(srcfile)
        d=FilePath(dstfile)
        if s.exists() and d.exists():
            if s.getsize() == d.getsize():
                log.msg("Ignore coping, files have the same size: %s -> %s" % (srcfile,dstfile ))
                self.decpendingqueue("SAME_SIZE", msg=msg)
                self.publish('file-transfer-ack', fileTransferReq, id=msg.id.getValue(), state=0, table=msg.table.getValue())
                return
        src = FileSender()
        
        if self.conf.get('transport', 'CHUNK_SIZE_POWER'):
            src.CHUNK_SIZE = 2 ** int(self.conf.get('transport', 'CHUNK_SIZE_POWER'))
            
        try:
            f=open(srcfile, 'r')

        except Exception, e:
            log.msg(str(e), logLevel=logging.ERROR)
#                f=None
            return _err_done(Failure(e), srcfile, dstfile, 'err').addBoth(self.decpendingqueue, msg=msg)
        
        ###Creating dst dir:
        try:
            d=FilePath(dstfile)
            d=FilePath(d.dirname())
            if not d.exists():
                d.makedirs()
        except Exception, e:
            log.msg(str(e), logLevel=logging.ERROR)
            return _err_done(Failure(e), srcfile, dstfile, 'err').addBoth(self.decpendingqueue, msg=msg)            
            
        dst=dstFile(dstfile)
        src.beginFileTransfer(f, dst).addCallback(dst.connectionLost).addErrback(_err_done, srcfile, dstfile).\
            addCallback(_done, srcfile, dstfile, f).addErrback(_err_done, srcfile, dstfile, sendreq=True).addBoth(dst.closeFile).addBoth(self.decpendingqueue, msg=msg)
#             src.beginFileTransfer(f, dst).addCallbacks(dst.connectionLost,_err_done, srcfile, dstfile).\
#                 addCallbacks(_done, _err_done, srcfile, dstfile, f, srcfile, dstfile, "err").addBoth(dst.closeFile)            
        
    
        
def run_transfer():
    
    app = service.Application('file-transfer')
    config = FileTransferOptions()
    try:
        config.parseOptions()
        import ConfigParser

        conf = ConfigParser.ConfigParser()
        conf.read(config['config'])        
        node = FileTransferNode(conf, config['passive'])
        node.start().addCallback(lambda _: node.do_subscribe())
        node.setServiceParent(app)
    
        runApp(config, app)
        
    except usage.error, e:
        print e
     
if __name__ == '__main__':    
    run_transfer()   
