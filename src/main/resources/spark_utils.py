class UtilWrapper:
    def listFiles(self, filepath):
        return sc._jvm.com.github.whaitukay.utils.UtilWrapper.listFiles(filepath)