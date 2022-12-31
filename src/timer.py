import random
import time
from threading import Timer

# when the timer runs out
# this executes the function specified
class ResettableTimer():
    def __init__(self, function, args, interval):
        self.interval = interval
        self.function = function
        # if args is None:
        #     self.timer = Timer(self.time_interval(), self.function)
        # # # runs the timer based on whatever the time_interval function returns
        # # # and then after it runs out execute the function
        # # # useful for sending heartbeat messages and waiting to reset
        # else:
        # print(f"function is {function}")
        # print(f"args is {args}")
        self.args = args
        self.timer = Timer(self.time_interval(), self.function, args=self.args)


    def time_interval(self):
        # starred expression here used to generate a random number between the
        # lower and upper bound as specified in self.interval
        # temporarily remove the /1000 to have greater lengths of time
        # for testing purposes
        # return random.randint(*self.interval)
        return random.randint(*self.interval) / 1000

    def run(self):
        self.timer.start()

    def reset(self):
        self.timer.cancel()
        x = self.time_interval()
        # print(f"timer ticking for {x}s")
        self.timer = Timer(self.time_interval(), self.function, args=self.args)
        self.timer.start()

    def stop(self):
        self.timer.cancel()

# def hello_world():
#     print("hello world!")
#
# def heartbeat(entry, peers):
#     print(entry)
#     print(peers)
#
# if __name__ == "__main__":
#     heartbeat_timer = ResettableTimer(function=heartbeat, args=([],[1,2,3]),interval=(1000,2000))
#     heartbeat_timer.stop()
#     heartbeat_timer.reset()
#     hello_timer = ResettableTimer(function=hello_world, args=None, interval=(1000,2000))
#     hello_timer.reset()