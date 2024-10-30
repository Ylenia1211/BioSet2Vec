import jpype
import os

jar_path = os.path.join(os.path.dirname(__file__), 'bioft.jar')

# Start the JVM with the jar file 
def start_jvm():
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[jar_path])




def shutdown_jvm():
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
