[![Coverage Status](https://coveralls.io/repos/github/python-discord/async-rediscache/badge.svg?branch=master)](https://coveralls.io/github/python-discord/async-rediscache?branch=master)
![Lint & Test](https://github.com/python-discord/async-rediscache/workflows/Lint%20&%20Test/badge.svg)
![Release to PyPI](https://github.com/python-discord/async-rediscache/workflows/Release%20to%20PyPI/badge.svg)

# Asynchronous Redis Cache
This package offers several data types to ease working with a Redis cache in an asynchronous workflow. The package is currently in development and it's not recommended to start using it in production at this point.

## Installation

### Prerequisites

To use `async-rediscache`, make sure that [`redis`](https://redis.io/download) is installed and running on your system. Alternatively, you could use `fakeredis` as a back-end for testing purposes and local development.

### Install using `pip`

To install `async-rediscache` run the following command:

```bash
pip install async-rediscache
```

Alternatively, to install `async-rediscache` with `fakeredis` run:

```bash
pip install async-rediscache[fakeredis]
```

## Basic use

### Creating a `RedisSession`
To use a `RedisCache`, you first have to create a `RedisSession` instance that manages the connection pool to Redis. You can create the `RedisSession` at any point but make sure to call the `connect` method from an asynchronous context (see [this explanation](https://docs.aiohttp.org/en/stable/faq.html#why-is-creating-a-clientsession-outside-of-an-event-loop-dangerous) for why).

```python
import async_rediscache

async def main():
    session = async_rediscache.RedisSession()
    await session.connect()

    # Do something interesting
    
    await session.close()
```

### Creating a `RedisSession` with a network connection

```python
async def main():
    connection = {"address": "redis://127.0.0.1:6379"}
    async_rediscache.RedisSession(**connection)
```
### `RedisCache`

A `RedisCache` is the most basic data type provided by `async-rediscache`. It works like a dictionary in that you can associate keys with values. To prevent key collisions, each `RedisCache` instance should use a unique `namespace` identifier that will be prepended to the key when storing the pair to Redis.

#### Creating a `RedisCache` instance

When creating a `RedisCache` instance, it's important to make sure that it has a unique `namespace`. This can be done directly by passing a `namespace` keyword argument to the constructor:

```python
import async_rediscache

birthday_cache = async_rediscache.RedisCache(namespace="birthday")
```

Alternatively, if you assign a class attribute to a `RedisCache` instance, a namespace will be automatically generated using the name of the owner class and the name of attribute assigned to the cache:

```python
import async_rediscache

class Channel:
    topics = async_rediscache.RedisCache()  # The namespace be set to `"Channel.topics"`
```

Note: There is nothing preventing you from reusing the same namespace, although you should be aware this could lead to key collisions (i.e., one cache could interfere with the values another cache has stored).

#### Using a `RedisCache` instance

Using a `RedisCache` is straightforward: Just call and await the methods you want to use and it should just work. There's no need to pass a `RedisSession` around as the session is fetched internally by the `RedisCache`. Obviously, one restriction is that you have to make sure that the `RedisSession` is still open and connected when trying to use a `RedisCache`.

Here are some usage examples:

```python
import async_rediscache

async def main():
    session = async_rediscache.RedisSession()
    await session.connect()

    cache = async_rediscache.RedisCache(namespace="python")

    # Simple key/value manipulation
    await cache.set("Guido", "van Rossum")
    print(await cache.get("Guido"))  # Would print `van Rossum`

    # A contains check works as well
    print(await cache.contains("Guido"))  # True
    print(await cache.contains("Kyle"))  # False

    # You can iterate over all key, value pairs as well:
    item_view = await cache.items()
    for key, value in item_view:
        print(key, value)

    # Other options:
    number_of_pairs = await cache.length()
    pairs_in_dict = await cache.to_dict()
    popped_item = await cache.pop("Raymond", "Default value")
    await cache.update({"Brett": 10, "Barry": False})
    await cache.delete("Barry")
    await cache.increment("Brett", 1)  # Increment Brett's int by 1
    await cache.clear()

    await session.close()
```

#### `RedisQueue`

A `RedisQueue` implements the same interface as a `queue.SimpleQueue` object, except that all the methods are coroutines. Creating an instance works the same as with a `RedisCache`. 
