from __future__ import print_function
from celery import Celery, chain, group
import time

celery = Celery(
    'pugdemo',
    broker='redis://localhost/1',
    backend='redis')

@celery.task
def say(*msg):
    print("".join(msg))

@celery.task
def increment(x):
    return x + 1

@celery.task
def square(x):
    return x ** 2

if __name__ == '__main__':

    # Run synchronously
    say('I am synchronous')

    # Run asynchronously. 
    # result.get() waits for task to finish
    result = increment.delay((5))
    print("5 + 1 is", result.get())

    # Make 'subtask' to run later with args
    # Args at call-time are prepended
    ask = say.s("?")
    task = ask("How are you")
    task.get()

    prep = group(
        say.si("slice bread"),
        say.si("slice cheese"),
        say.si("put butter in pan"))

    grilledcheese = chain(
        prep | 
        say.si("turn on burner") |
        say.si("assemble") |
        say.si("cook one side") |
        say.si("cook other side"))

    grilledcheese()

    add_three_and_square = chain(
        increment.s() |
        increment.s() |
        increment.s() |
        square.s())

    print("Add three and square: ", add_three_and_square)
    res = add_three_and_square(5)
    print("(5 + 3) ^ 2 =", res.get())
    


