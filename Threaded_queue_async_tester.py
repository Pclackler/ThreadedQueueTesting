import queue
import threading
import time
import asyncio
import random
import numpy as np
import sys

# Okay so the point of this is to create a quick and dirty working example for the framework of an automated trading system
# that relies on using a synchronous event loop combines with an asyncio asynchronous event loop.

# I have carefully chosen the palcement of the timers so that ONLY the latency of passing the messages through the queue 
# is measured. The goal is to have the most consistent and LOWEST latency possible for INDIVIDUAL messages that will be small.

# So i have to fight the intepreted language style of python and dynamic type casting and mem management and all that
# which creates tiny tiny little bits of overhead on each operation. I need to identify and reduce these.

# The async_write() methods SIMULATE the receiving of websocket trade message which is the foundation of the bot.
# These methods 'receive' trade messages from their respective exchanges in a real world manner using random numbers representing
# Buy or Sell messages at a random price, with TWO layers of random delays between the messages.
# so when you run this, every second a new random weight is generated, each random delay is multiplied by that weight.
# Which creates real world scenarios where there will be periods of low and high volitility. 

# ... and success!
# it actually does look a lot like this when you stream trade messages from multiple exchanges. You get periods of very little trading and then
# an avalanche of trades.

# I will implement some logic in here that will just halt the program if it encounters a queue latecy of over 2ms and see how many days this
# will run.

# from my research other methods oF IPC in python are slow and weird, and have a lot of latency. 

# So far a queue object wins!
 
# The while True loop in global namespace will represent the main brain of my software and i will 
# just manage threads in the global namespace like this as it is the fastest way python operates to my 
# Knowledge


# RESULTS :
# so far it stays between 0 and 5ms.
# The idea is to reduce the number of high latencies as this disrupts processing of other methods

# Over years on end, milliseconds will translate to ALOT of dollars lost when it comes to actually 
# submitting market orders and getting order book preference when trying to close positions.

global_latencies = []
rnd_weight = 0.0

def reader_thread(queue_object):

    #grab global latencies from global namespace so we can read/modify properly
    global global_latencies

    while True:
        item = queue_object.get()  # Get an item from the queue
        
        #retreive the timestamp from the queue item which was stored BEFORE 
        #being serialized into the queue and calc the change to get our queue latency AFTER its been loaded out of the queue. I think this is the correct way to calculate it
        ts_diff = (time.time() - item['ts'])  * 1000

        queue_object.task_done()  # Signal that the item has been processed

        print("Reader Thread:" + item['data'] + ' queue latency:' + str(ts_diff) + 'ms')
        
        if ts_diff > 5.0:
            print("LATECY OF OVER 10ms DETECED - HALTING OPERATIONS")
            sys.exit(0)
    
        
     

# Here's our 'simulated' websocket streamer methods. Bundle them up and run them in the asyncio event loop
async def async_handler(queue_object):
    loop = asyncio.get_event_loop()
    tasks = [
        async_write_1(queue_object),
        async_write_2(queue_object),
        async_write_3(queue_object),
        async_write_4(queue_object),
        async_write_5(queue_object),
        async_write_6(queue_object),
        async_write_7(queue_object)
        ]
    await asyncio.gather(*tasks)
    

def writer_thread(queue_object):
    asyncio.run(async_handler(queue_object))


async def async_write_1(queue_object):
    while True:
        global rnd_weight
        data = ('BTC BUY - Binance price: ' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - Binance price: ' + str(random.uniform(0.1,983585837738573.568))
        payload = {'ts' : time.time() , 'data' : data}
        queue_object.put(payload)

        await asyncio.sleep( random.uniform(0.0001, 8) * rnd_weight)

async def async_write_2(queue_object):
    while True:
        data = ('BTC BUY - Coinbase price: ' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - Coinbase price: ' + str(random.uniform(0.1,983585837738573.568))
        payload = {'ts' : time.time() , 'data' : data}
        queue_object.put(payload)
        await asyncio.sleep(random.uniform(0.0001, 13) * rnd_weight)

async def async_write_3(queue_object):
    while True:
        data = ('BTC BUY - Kraken price: ' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - Kraken price: ' + str(random.uniform(0.1,983585837738573.568))
        payload = {'ts' : time.time() , 'data' : data}
        queue_object.put(payload)
        await asyncio.sleep(random.uniform(0.0001, 15) * rnd_weight)

async def async_write_4(queue_object):
    while True:
        data = ('BTC BUY - Huobi price: ' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - Huobi price: ' + str(random.uniform(0.1,928358837738573.568))
        payload = {'ts' : time.time() , 'data' : data}
        queue_object.put(payload)
        await asyncio.sleep(random.uniform(0.0001, 15) * rnd_weight)

async def async_write_5(queue_object):
    while True:
        data = ('BTC BUY - OKEX price: ' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - OKEX price: ' + str(random.uniform(0.1,983585837738573.568))
        payload = {'ts' : time.time() , 'data' : data}
        queue_object.put(payload)
        await asyncio.sleep(random.uniform(0.0001, 15) * rnd_weight)

async def async_write_6(queue_object):
    while True:
        data = ('BTC BUY - Gemini price: ' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - Gemini price: ' + str(random.uniform(0.1,983585837738573.568))
        payload = {'ts' : time.time() , 'data' : data}
        queue_object.put(payload)
        await asyncio.sleep(random.uniform(0.0001, 15) * rnd_weight)

async def async_write_7(queue_object):
    while True:
        data = ('BTC BUY - Binance.us price: ' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - Binance.US price: ' + str(random.uniform(0.1,983585837738573.568))
        payload = {'ts' : time.time() , 'data' : data}
        queue_object.put(payload)
        await asyncio.sleep(random.uniform(0.0001, 15) * rnd_weight)


# Create a queue
queue_object = queue.Queue()

# Create and start the reader thread
reader = threading.Thread(target=reader_thread, args=(queue_object,))
reader.start()

# Create and start the writer thread
writer = threading.Thread(target=writer_thread, args=(queue_object,))
writer.start()

# This is the main 'brain' of it all. This is where I can implement all kinds of error checking and 
# restarting of threads and synchronization things too.

# having this mess sitting in global namespace is better than passing it around memery through a bunch of 
# classes. 


while True:
    rnd_weight = random.uniform(0.0001, .1)

    if not reader.is_alive():
        print("Reader thread has terminated unexpectedly.")
        # Take appropriate action like logging, restarting the thread, or stopping the program
    else:
        print('reader is alive')

    if not writer.is_alive():
        print("Writer thread has terminated unexpectedly.")
        # Take appropriate action like logging, restarting the thread, or stopping the program
    else:
        print('write is alive')
    # Sleep for some time before checking the thread status again
    time.sleep(1)


# Wait for both threads to finish
reader.join()
writer.join()
