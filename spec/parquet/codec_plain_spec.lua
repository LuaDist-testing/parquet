local parquet_codec_plain = require 'parquet.codec.plain'
local registerAsserts = require 'registerAsserts'

registerAsserts(assert)

local makeBinaryString = function(s)
  return string.gsub(s, '\\x(%x%x)', function (x) return string.char(tonumber(x,16)) end)
end

describe('ParquetCodec::PLAIN', function()

--  it('should encode BOOLEAN values', function()
--    local buf = parquet_codec_plain.encodeValues(
--        'BOOLEAN',
--        {true, false, true, true, false, true, false, false})
--
--    assert.same(buf, makeBinaryString('\\x2d')) -- b101101
--  end)

  it('should decode BOOLEAN values', function()
    local buf = {
      offset=0,
      buffer=makeBinaryString('\\x2d') -- b101101
    }
    local vals = parquet_codec_plain.decodeValues('BOOLEAN', buf, 8, {})
    assert.equal(1, buf.offset)
    assert.same(vals, {true, false, true, true, false, true, false, false})
  end)

  it('should encode INT32 values', function()
    local buf = parquet_codec_plain.encodeValues(
        'INT32',
        {42, 17, 23, -1, -2, -3, 9000, 420})

    assert.same(buf, makeBinaryString(table.concat({
      '\\x2a\\x00\\x00\\x00', -- 42
      '\\x11\\x00\\x00\\x00', -- 17
      '\\x17\\x00\\x00\\x00', -- 23
      '\\xff\\xff\\xff\\xff', -- -1
      '\\xfe\\xff\\xff\\xff', -- -2
      '\\xfd\\xff\\xff\\xff', -- -3
      '\\x28\\x23\\x00\\x00', -- 9000
      '\\xa4\\x01\\x00\\x00'  -- 420
    })))
  end)

  it('should decode INT32 values', function()
    local buf = {
      offset=0,
      buffer=makeBinaryString(table.concat({
        '\\x2a\\x00\\x00\\x00', -- 42
        '\\x11\\x00\\x00\\x00', -- 17
        '\\x17\\x00\\x00\\x00', -- 23
        '\\xff\\xff\\xff\\xff', -- -1
        '\\xfe\\xff\\xff\\xff', -- -2
        '\\xfd\\xff\\xff\\xff', -- -3
        '\\x28\\x23\\x00\\x00', -- 9000
        '\\xa4\\x01\\x00\\x00'  -- 420
      }))
    }

    local vals = parquet_codec_plain.decodeValues('INT32', buf, 8, {})
    assert.equal(32, buf.offset)
    assert.same(vals, {42, 17, 23, -1, -2, -3, 9000, 420})
  end)

--  it('should encode INT64 values', function()
--    local buf = parquet_codec_plain.encodeValues(
--        'INT64',
--        [42, 17, 23, -1, -2, -3, 9000, 420])
--
--    assert.deepEqual(buf, new Buffer([
--      0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 42
--      0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 17
--      0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 23
--      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, -- -1
--      0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, -- -2
--      0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, -- -3
--      0x28, 0x23, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 9000
--      0xa4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  -- 420
--    ]))
--  end)

--  it('should decode INT64 values', function()
--    local buf = {
--      offset=0,
--      buffer=makeBinaryString(table.concat({
--        '\\x2a\\x00\\x00\\x00\\x00\\x00\\x00\\x00', -- 42
--        '\\x11\\x00\\x00\\x00\\x00\\x00\\x00\\x00', -- 17
--        '\\x17\\x00\\x00\\x00\\x00\\x00\\x00\\x00', -- 23
--        '\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xff', -- -1
--        '\\xfe\\xff\\xff\\xff\\xff\\xff\\xff\\xff', -- -2
--        '\\xfd\\xff\\xff\\xff\\xff\\xff\\xff\\xff', -- -3
--        '\\x28\\x23\\x00\\x00\\x00\\x00\\x00\\x00', -- 9000
--        '\\xa4\\x01\\x00\\x00\\x00\\x00\\x00\\x00'  -- 420
--      }))
--    }
--
--    local vals = parquet_codec_plain.decodeValues('INT64', buf, 8, {})
--    assert.equal(64, buf.offset)
--    assert.same(vals, {42, 17, 23, -1, -2, -3, 9000, 420})
--  end)

--  it('should encode INT96 values', function()
--    local buf = parquet_codec_plain.encodeValues(
--        'INT96',
--        [42, 17, 23, -1, -2, -3, 9000, 420])
--
--    assert.deepEqual(buf, new Buffer([
--      0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 42
--      0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 17
--      0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 23
--      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, -- -1
--      0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, -- -2
--      0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, -- -3
--      0x28, 0x23, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 9000
--      0xa4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  -- 420
--    ]))
--  end)
--
--  it('should decode INT96 values', function()
--    local buf = {
--      offset: 0,
--      buffer: new Buffer([
--        0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 42
--        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 17
--        0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 23
--        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, -- -1
--        0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, -- -2
--        0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, -- -3
--        0x28, 0x23, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, -- 9000
--        0xa4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  -- 420
--      ])
--    }
--
--    local vals = parquet_codec_plain.decodeValues('INT96', buf, 8, {})
--    assert.equal(buf.offset, 96)
--    assert.deepEqual(vals, [42, 17, 23, -1, -2, -3, 9000, 420])
--  end)

--  it('should encode FLOAT values', function()
--    local buf = parquet_codec_plain.encodeValues(
--        'FLOAT',
--        {42.0, 23.5, 17.0, 4.20, 9000})
--
--    assert.equal(buf, makeBinaryString(table.concat({
--      '\\x00\\x00\\x28\\x42', -- 42.0
--      '\\x00\\x00\\xbc\\x41', -- 23.5
--      '\\x00\\x00\\x88\\x41', -- 17.0
--      '\\x66\\x66\\x86\\x40', -- 4.20
--      '\\x00\\xa0\\x0c\\x46'  -- 9000
--    })))
--  end)

  it('should decode FLOAT values', function()
    local buf = {
      offset=0,
      buffer=makeBinaryString(table.concat({
        '\\x00\\x00\\x28\\x42', -- 42.0
        '\\x00\\x00\\xbc\\x41', -- 23.5
        '\\x00\\x00\\x88\\x41', -- 17.0
        '\\x66\\x66\\x86\\x40', -- 4.20
        '\\x00\\xa0\\x0c\\x46'  -- 9000
      }))
    }

    local vals = parquet_codec_plain.decodeValues('FLOAT', buf, 5, {})
    assert.equal(20, buf.offset)
    assert.equal_absTol(vals, {42.0, 23.5, 17.0, 4.20, 9000})
  end)

--  it('should encode DOUBLE values', function()
--    local buf = parquet_codec_plain.encodeValues(
--        'DOUBLE',
--        [42.0, 23.5, 17.0, 4.20, 9000])
--
--    assert.deepEqual(buf, new Buffer([
--      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x45, 0x40, -- 42.0
--      0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x37, 0x40, -- 23.5
--      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x40, -- 17.0
--      0xcd, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0x10, 0x40, -- 4.20
--      0x00, 0x00, 0x00, 0x00, 0x00, 0x94, 0xc1, 0x40  -- 9000
--    ]))
--  end)

  it('should decode DOUBLE values', function()
    local buf = {
      offset=0,
      buffer=makeBinaryString(table.concat({
        '\\x00\\x00\\x00\\x00\\x00\\x00\\x45\\x40', -- 42.0
        '\\x00\\x00\\x00\\x00\\x00\\x80\\x37\\x40', -- 23.5
        '\\x00\\x00\\x00\\x00\\x00\\x00\\x31\\x40', -- 17.0
        '\\xcd\\xcc\\xcc\\xcc\\xcc\\xcc\\x10\\x40', -- 4.20
        '\\x00\\x00\\x00\\x00\\x00\\x94\\xc1\\x40'  -- 9000
      }))
    }

    local vals = parquet_codec_plain.decodeValues('DOUBLE', buf, 5, {})
    assert.equal(40, buf.offset)
    assert.equal_absTol(vals, {42.0, 23.5, 17.0, 4.20, 9000})
  end)

--  it('should encode BYTE_ARRAY values', function()
--    local buf = parquet_codec_plain.encodeValues(
--        'BYTE_ARRAY',
--        ['one', new Buffer([0xde, 0xad, 0xbe, 0xef]), 'three'])
--
--    assert.deepEqual(buf, new Buffer([
--      0x03, 0x00, 0x00, 0x00,       -- (3)
--      0x6f, 0x6e, 0x65,             -- 'one'
--      0x04, 0x00, 0x00, 0x00,       -- (4)
--      0xde, 0xad, 0xbe, 0xef,       -- 0xdeadbeef
--      0x05, 0x00, 0x00, 0x00,       -- (5)
--      0x74, 0x68, 0x72, 0x65, 0x65  -- 'three'
--    ]))
--  end)
--
--  it('should decode BYTE_ARRAY values', function()
--    local buf = {
--      offset: 0,
--      buffer: new Buffer([
--        0x03, 0x00, 0x00, 0x00,       -- (3)
--        0x6f, 0x6e, 0x65,             -- 'one'
--        0x04, 0x00, 0x00, 0x00,       -- (4)
--        0xde, 0xad, 0xbe, 0xef,       -- 0xdeadbeef
--        0x05, 0x00, 0x00, 0x00,       -- (5)
--        0x74, 0x68, 0x72, 0x65, 0x65  -- 'three'
--      ])
--    }
--
--    local vals = parquet_codec_plain.decodeValues('BYTE_ARRAY', buf, 3, {})
--    assert.equal(buf.offset, 24)
--    assert.deepEqual(vals, [
--      Buffer.from('one'),
--      new Buffer([0xde, 0xad, 0xbe, 0xef]),
--      Buffer.from('three')
--    ])
--  end)

end)
