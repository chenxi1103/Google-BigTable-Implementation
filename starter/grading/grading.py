import sys, requests, unittest
from cp1_TableTests import TableTests
from cp1_OpTests import OpTests
from cp1_KillTests import KillTests
from cp1_StressTests import StressTests
from cp2_MasterTests import MasterTests
from cp2_SpecialTests import SpecialTests
from cp2_BalancerTests import BalancerTests

def checkpoint1(hostname, port):
    total = 0

    print("Running tests for Checkpoint 1")

    TableTests.HOSTNAME = hostname
    TableTests.PORT = port
    runner = unittest.TextTestRunner()
    runner.run(TableTests.suite())

    OpTests.HOSTNAME = hostname
    OpTests.PORT = port
    runner = unittest.TextTestRunner()
    runner.run(OpTests.suite())

    StressTests.HOSTNAME = hostname
    StressTests.PORT = port
    runner = unittest.TextTestRunner()
    runner.run(StressTests.suite())

    KillTests.HOSTNAME = hostname
    KillTests.PORT = port
    runner = unittest.TextTestRunner()
    runner.run(KillTests.suite())

    return total


def checkpoint2(hostname, port):
    total = 0

    print("Running tests for Checkpoint 2")

    MasterTests.HOSTNAME = hostname
    MasterTests.PORT = port
    runner = unittest.TextTestRunner()
    runner.run(MasterTests.suite())

    BalancerTests.HOSTNAME = hostname
    BalancerTests.PORT = port
    runner = unittest.TextTestRunner()
    runner.run(BalancerTests.suite())

    SpecialTests.HOSTNAME = hostname
    SpecialTests.PORT = port
    runner = unittest.TextTestRunner()
    runner.run(SpecialTests.suite())

    return total


def run_testcase(testcase, hostname, port):
    print("Running Test Case: " + testcase)

    if testcase == "Tablet":
        TabletTests.HOSTNAME = hostname
        TabletTests.PORT = port
        runner = unittest.TextTestRunner()
        runner.run(TabletTests.suite())
    elif testcase == "Op":
        OpTests.HOSTNAME = hostname
        OpTests.PORT = port
        runner = unittest.TextTestRunner()
        runner.run(OpTests.suite())
    elif testcase == "Stress":
        StressTests.HOSTNAME = hostname
        StressTests.PORT = port
        runner = unittest.TextTestRunner()
        runner.run(StressTests.suite())
    elif testcase == "Kill":
        KillTests.HOSTNAME = hostname
        KillTests.PORT = port
        runner = unittest.TextTestRunner()
        runner.run(KillTests.suite())
    elif testcase == "Master":
        MasterTests.HOSTNAME = hostname
        MasterTests.PORT = port
        runner = unittest.TextTestRunner()
        runner.run(MasterTests.suite())
    elif testcase == "Balancer":
        BalancerTests.HOSTNAME = hostname
        BalancerTests.PORT = port
        runner = unittest.TextTestRunner()
        runner.run(BalancerTests.suite())
    elif testcase == "Special":
        SpecialTests.HOSTNAME = hostname
        SpecialTests.PORT = port
        runner = unittest.TextTestRunner()
        runner.run(SpecialTests.suite())
    else:
        print("Test Case: " + testcase + " Not Recognized")


def usage():
    print("Welcome to Bigtable grading script...")
    print("For Checkpoint 1: ")
    print("\tpython grading.py 1 <tablet server hostname> <tablet server port>")
    print("For Final Submission: ")
    print("\tpython grading.py 2 <master server hostname> <master server port>")
    print("For Specific Test Case: ")
    print("\tpython grading.py t <test case> <master server hostname> <master server port>")
    print("\tTablet Test Case: Tablet | Op | Stress | Kill")
    print("\tMaster Test Case: Master | Balancer | Special")
    print("\tWithin a test case, later test cases may rely on earlier ones")
    print("\tIf commenting out tests for development, preserve order")
    print("\t(If you want a test to run, all earlier tests in the test case should also run)")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        usage()
        exit(1)

    checkpoint = sys.argv[1]

    if checkpoint == "1":
        hostname = sys.argv[2]
        port = sys.argv[3]
        checkpoint1(hostname, port)
    elif checkpoint == "2":
        hostname = sys.argv[2]
        port = sys.argv[3]
        checkpoint2(hostname, port)
    elif checkpoint == "t":
        if len(sys.argv) < 5:
            usage()
            exit(1)
        testcase = sys.argv[2]
        hostname = sys.argv[3]
        port = sys.argv[4]
        run_testcase(testcase, hostname, port)
    else :
        usage()
