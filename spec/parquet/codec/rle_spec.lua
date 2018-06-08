-- lifted from https://github.com/ironSource/parquetjs/commit/009c87f731e5264789e78b3ed8ea23f8b0246eb8,
-- which is newer than 0.8.0 tag

local parquet_codec_rle = require 'parquet.codec.rle'
local registerAsserts = require 'registerAsserts'

registerAsserts(assert)

describe('ParquetCodec::RLE', function()

--  it('should encode BOOLEAN values', function()
--    local buf = parquet_codec_plain.encodeValues(
--        'BOOLEAN',
--        {true, false, true, true, false, true, false, false})
--
--    assert.same(buf, makeBinaryString('\\x2d')) -- b101101
--  end)

--  it('should encode bitpacked values', function()
--    local buf = parquet_codec_rle.encodeValues(
--        'INT32',
--        {0, 1, 2, 3, 4, 5, 6, 7},
--        {
--          disableEnvelope = true,
--          bitWidth = 3
--        })
--
--    assert.same(buf, makeBinaryString('\\x03\\x88\\xc6\\xfa'))
--  end)

  it('should decode bitpacked values', function()
    local vals = parquet_codec_rle.decodeValues(
        'INT32',
        {offset=0, buffer={0x03, 0x88, 0xc6, 0xfa}},
        8,
        {disableEnvelope=true, bitWidth=3})
    assert.same({0, 1, 2, 3, 4, 5, 6, 7}, vals)
  end)

--  it('should encode repeated values', function()
--    local buf = parquet_codec_rle.encodeValues(
--        'INT32',
--        {42, 42, 42, 42, 42, 42, 42, 42},
--        {
--          disableEnvelope: true,
--          bitWidth: 6
--        })
--
--    assert.deepEqual(buf, new Buffer({0x10, 0x2a}))
--  })

  it('should decode repeated values', function()
    local vals = parquet_codec_rle.decodeValues(
      'INT32',
      {offset=0, buffer={0x10, 0x2a}},
      8,
      {disableEnvelope=true, bitWidth=3})
    assert.same({42,42,42,42,42,42,42,42}, vals)
  end)

--  it('should encode mixed runs', function()
--    local buf = parquet_codec_rle.encodeValues(
--        'INT32',
--        {0, 1, 2, 3, 4, 5, 6, 7, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 3, 4, 5, 6, 7},
--        {
--          disableEnvelope: true,
--          bitWidth: 3
--        })
--
--    assert.deepEqual(buf, new Buffer({0x03, 0x88, 0xc6, 0xfa, 0x10, 0x04, 0x03, 0x88, 0xc6, 0xfa}))
--  })

  it('should decode mixed runs', function()
    local vals = parquet_codec_rle.decodeValues(
      'INT32',
      {offset=0, buffer={0x03, 0x88, 0xc6, 0xfa, 0x10, 0x04, 0x03, 0x88, 0xc6, 0xfa}},
      24,
      {disableEnvelope=true, bitWidth=3})
    assert.same({0,1,2,3,4,5,6,7,4,4,4,4,4,4,4,4,0,1,2,3,4,5,6,7}, vals)
  end)

end)
