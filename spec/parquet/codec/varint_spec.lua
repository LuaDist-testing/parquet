local bit32 = require 'bit32'
local util = require 'parquet.util'
local varint = require 'parquet.codec.varint'

local decode, encode, encodingLength = varint.decode, varint.encode, varint.encodingLength

local function randint(range)
  return math.random(range)
end

describe('parquet.codec.varint', function()

  it('fuzz test', function()
    for i=1,100 do
      local expect = randint(0x7FFFFFFF)
      local encoded = encode(expect)
      local data, decodeBytes = decode(encoded)
      assert.equal(expect, data)
      assert.equal(decodeBytes, #encoded)
    end
  end)
  
  it('test single byte works as expected', function()
    local buf = {172, 2}
    local data, decodeBytes = decode(buf)
    assert.equal(300, data)
    assert.equal(2, decodeBytes)
  end)
  
  it('test encode works as expected', function()
    assert.same({0xAC, 0x02}, encode(300))
  end)
  
  it('test decode single bytes', function()
    local expected = randint(127)
    local buf = {expected}
    local data, decodeBytes = decode(buf)
    assert.equal(expected, data)
    assert.equal(1, decodeBytes)
  end)
  
  it('test decode multiple bytes with zero', function()
    local expected = 127 -- %1111111
    local buf = {128, expected}
    local data, decodeBytes = decode(buf)
    assert.equal(bit32.lshift(expected, 7), data)
    assert.equal(2, decodeBytes)
  end)
  
  it('encode single byte', function()
    local expected = randint(127)
    local actual, encodeBytes = encode(expected)
    assert.same({expected}, actual)
    assert.equal(1, encodeBytes)
  end)
  
  it('encode multiple byte with zero first byte', function()
    local expected = 0x0F00
    local actual, encodeBytes = encode(expected)
    assert.same({0x80, 0x1E}, actual)
    assert.equal(2, encodeBytes)
  end)
  
  --it('big integers', function (assert) {
  --  local bigs = []
  --  for(local i = 32; i <= 53; i++) (function (i) {
  --    bigs.push(Math.pow(2, i) - 1)
  --    bigs.push(Math.pow(2, i))
  --  })(i)
  --  bigs.forEach(function (n) {
  --    local data = encode(n)
  --    console.error(n, '->', data)
  --    assert.equal(decode(data), n)
  --    assert.notEqual(decode(data), n - 1)
  --  })
  --  assert.end()
  --})
  --
  --it('fuzz test - big', function()
  --  local expect
  --    , encoded
  --  local MAX_INTD = Math.pow(2, 55)
  --  local MAX_INT = Math.pow(2, 31)
  --  for(local i = 0, len = 100; i < len; ++i) {
  --    expect = randint(MAX_INTD - MAX_INT) + MAX_INT
  --    encoded = encode(expect)
  --    local data = decode(encoded)
  --    assert.equal(expect, data, 'fuzz test: ' + expect.toString())
  --    assert.equal(decode.bytes, encoded.length)
  --  }
  --  assert.end()
  --})
  
  it('encodingLength', function ()
    for i=0,53 do
      local n = math.pow(2, i)
      assert.equal(encodingLength(n), #encode(n))
    end
  end)
  
  it('buffer too short', function ()
    local value = encode(9812938912312)
    local buffer = encode(value)
    for l=#buffer,0,-1 do
      assert.has_error(function()
        decode(util.slice(buffer, 0, l))
      end)
    end
  end)
  
end)
