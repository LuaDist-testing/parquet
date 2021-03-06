local parquet_util = require 'parquet.util'

describe('util', function()

  it('splice removes 1 element from index 4', function()
    -- https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/splice
    local myFish = {'angel', 'clown', 'drum', 'mandarin', 'sturgeon'}
    local removed = parquet_util.splice(myFish, 4, 1)
    assert.same({'mandarin'}, removed)
    assert.same({'angel', 'clown', 'drum', 'sturgeon'}, myFish)
  end)

  it('splice removes 2 elements from index 3', function()
    local myFish = {'parrot', 'anemone', 'blue', 'trumpet', 'sturgeon'}
    local removed = parquet_util.splice(myFish, 3, 2)
    assert.same({'parrot', 'anemone', 'sturgeon'}, myFish)
    assert.same({'blue', 'trumpet'}, removed)
  end)

  it('splice removes all elements after index 3 (inclusive)', function()
    local myFish = {'angel', 'clown', 'mandarin', 'sturgeon'}
    local removed = parquet_util.splice(myFish, 3)
    assert.same({'angel', 'clown'}, myFish)
    assert.same({'mandarin', 'sturgeon'}, removed)
  end)

  it('slice', function()
    -- https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/slice
    local animals = {'ant', 'bison', 'camel', 'duck', 'elephant'}
    assert.same({'camel', 'duck', 'elephant'}, parquet_util.slice(animals, 3))
    assert.same({'camel', 'duck'}, parquet_util.slice(animals, 3, 5))
    assert.same({'bison', 'camel', 'duck', 'elephant'}, parquet_util.slice(animals, 2, 6))
  end)

  it('slice returns a portion of an existing array', function()
    -- https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/slice
    local fruits = {'Banana', 'Orange', 'Lemon', 'Apple', 'Mango'}
    local citrus = parquet_util.slice(fruits, 2, 4)
    assert.same({'Orange', 'Lemon'}, citrus)
  end)

  it('more slice', function()
    local a = {'1:id', '2:point', '3:type', '4:size', '5:indices', '6:list', '7:element', '8:values', '9:list', '10:element'}
    local b = parquet_util.slice(a, 3, 8)
    assert.same({'3:type', '4:size', '5:indices', '6:list', '7:element'}, b)
    local c = parquet_util.slice(b, 4, 6)
    assert.same({'6:list', '7:element'}, c)
    local d = parquet_util.slice(c, 2,4)
    assert.same({'7:element'}, d)
  end)
  
end)
