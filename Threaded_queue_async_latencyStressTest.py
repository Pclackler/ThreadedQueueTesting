import queue
import threading
import time
import asyncio
import random


global_latencies = []

# This is a pure queue latency test. 
# Modify this number to generate more trades to pass through the queue and test the latency of it being passed through the queue.

# right now its 10 million messages which i think takes longer to generate the list then it does
# To pass it through the queue
num_trades = 10000000

# Right now it will generate and pass a million 'trades' through the queue with a latency of around 8ms which i think is
# pretty good i guess? Since the data is being passed as objects that retain datatypes and it isnt being passed
# as some serialized binary format that is hard to work with. Queues are easy.


# Passing TEN MILLION item list 
# causes about 10-30ms queue latency.

# Given that I will never ever need to pass that much
# data at once, and the fact i actually need to pass thousands of items
# through a queue with their INDIVIDUAL latencies as low as possible
# I need another test program.

# The while True loop in global namespace will represent the main brain of my software and i will 
# just manage threads in the global namespace like this as it is the fastest way python operates to my 
# Knowledge
def reader_thread(q):

    #grab global latencies from global namespace so we can read/modify properly
    global global_latencies

    while True:
        item = q.get()  # Get an item from the queue
        
        ts_diff = (time.time() - item['ts'])  * 1000
        print("Reader Thread:", str(len(item['data'])) + ' queue latency:' + str(ts_diff) + 'ms')
        q.task_done()  # Signal that the item has been processed

async def async_handler(queue_object):
    loop = asyncio.get_event_loop()
    tasks = [async_write_1(queue_object),async_write_2(queue_object),async_write_3(queue_object)]
    await asyncio.gather(*tasks)
    

def writer_thread(q):
    asyncio.run(async_handler(q))
   

async def async_write_1(queue_object):
    while True:
        global num_trades 
        print(num_trades)
        listy = []
        for x in range(num_trades):
            listy.append(('BTC BUY - Binance' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - Binance' + str(random.uniform(0.1,983585837738573.568)))

        payload = {'ts' : time.time() , 'data' : listy}
        queue_object.put(payload)
        await asyncio.sleep(random.uniform(0.0001, 30.4))

async def async_write_2(queue_object):
    while True:
        global num_trades 
        listy = []
        for x in range(999999):
            listy.append(('BTC BUY - Coinbase' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - Binance' + str(random.uniform(0.1,983585837738573.568)))

        payload = {'ts' : time.time() , 'data' : listy}
        queue_object.put(payload)
        await asyncio.sleep(random.uniform(0.0001, 20.3))



async def async_write_3(queue_object):
    while True:
        global num_trades 
        listy = []
        for x in range(999999):
            listy.append(('BTC BUY - Kraken' + str(random.uniform(0.1, 983585837738573.568))) if random.uniform(0.1, .9) > .5 else 'BTC SELL - Binance' + str(random.uniform(0.1,983585837738573.568)))

        payload = {'ts' : time.time() , 'data' : listy}
        queue_object.put(payload)
        await asyncio.sleep(random.uniform(0.0001, .8))


# Create a queue
q = queue.Queue()

# Create and start the reader thread
reader = threading.Thread(target=reader_thread, args=(q,))
reader.start()

# Create and start the writer thread
writer = threading.Thread(target=writer_thread, args=(q,))
writer.start()

while True:
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
