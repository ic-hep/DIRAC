import os
from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement
from DIRAC.Resources.Computing.glexecComputingElement import glexecComputingElement


class glexecInProcessComputingElement(glexecComputingElement, InProcessComputingElement):
    def __init__(self, ceUniqueID):
        super(glexecInProcessComputingElement, self).__init__(ceUniqueID)
        self.in_process = False

    def submitJob(self, executableFile, proxy, dummy=None):
        original_dir = os.getcwd()
        try:
            result = glexecComputingElement.submitJob(self, executableFile, proxy, dummy)
        except Exception as e:
            self.log.exception(e)
            result = S_ERROR(str(e))

        if not result['OK']:
            self.log.error("Failed to submit job using glexecComputingElement: %s" % result['Message'])
            self.log.notice("faling back to InProcessComputingElement...")
            self.in_process = True
            os.chdir(original_dir)  # change out of the glexec dir into the original dir
            result = InProcessComputingElement.submitJob(self, executableFile, proxy, dummy)
            if not result['OK']:
                self.log.error("Failed to submit job using InProcessComputingElement as fallback: %s" % result['Message'])
                return result
        return S_OK()

    def monitorProxy(self, *args, **kwargs):
        if self.in_process:
            return InProcessComputingElement.monitorProxy(self, *args, **kwargs)
        return glexecComputingElement.monitorProxy(self, *args, **kwargs)
