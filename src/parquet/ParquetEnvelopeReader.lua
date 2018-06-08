local class = require 'middleclass'
--const parquet_codec = require('./codec')
--const parquet_compression = require('./compression')
local parquet_thrift = require 'parquet.parquet_ttypes'
local parquet_util = require 'parquet.util'
local vstruct = require 'vstruct'

  --[[
 * Parquet File Magic String
--]]
local PARQUET_MAGIC = 'PAR1'

--[[
 * The parquet envelope reader allows direct, unbuffered access to the individual
 * sections of the parquet file, namely the header, footer and the row groups.
 * This class is intended for advanced/internal users; if you just want to retrieve
 * rows from a parquet file use the ParquetReader instead
--]]
local ParquetEnvelopeReader = class('ParquetEnvelopeReader')

function ParquetEnvelopeReader.openFile(filePath)
  local fileDescriptor = parquet_util.fopen(filePath)
  local fileSize = parquet_util.fsize(fileDescriptor)
  local readFn = function(position, length) return parquet_util.fread(fileDescriptor, position, length) end
  local closeFn = function() return parquet_util.fclose(fileDescriptor) end
  return ParquetEnvelopeReader:new(readFn, closeFn, fileSize)
end

function ParquetEnvelopeReader:initialize(readFn, closeFn, fileSize)
  self.read = readFn
  self.close = closeFn
  self.fileSize = fileSize
end

function ParquetEnvelopeReader:close()
  self.close()
end

function ParquetEnvelopeReader:readHeader()
  local buf = self.read(0, #PARQUET_MAGIC)
  assert(buf == PARQUET_MAGIC, 'not valid parquet file')
end

--async readRowGroup(schema, rowGroup, columnList) {
function ParquetEnvelopeReader:readRowGroup()
  error('not implemented yet')
  --  var buffer = {
  --    rowCount: +rowGroup.num_rows,
  --    columnData: {}
  --  };
  --
  --  for (let colChunk of rowGroup.columns) {
  --    const colMetadata = colChunk.meta_data;
  --    const colKey = colMetadata.path_in_schema;
  --
  --    if (columnList.length > 0 && parquet_util.fieldIndexOf(columnList, colKey) < 0) {
  --      continue;
  --    }
  --
  --    buffer.columnData[colKey] = await this.readColumnChunk(schema, colChunk);
  --  }
  --
  --  return buffer;
end

--async readColumnChunk(schema, colChunk) {
function ParquetEnvelopeReader:readColumnChunk()
  error('not implemented yet')
--    if (colChunk.file_path !== null) {
--      throw 'external references are not supported';
--    }
--
--    let field = schema.findField(colChunk.meta_data.path_in_schema);
--    let type = parquet_util.getThriftEnum(
--        parquet_thrift.Type,
--        colChunk.meta_data.type);
--
--    let compression = parquet_util.getThriftEnum(
--        parquet_thrift.CompressionCodec,
--        colChunk.meta_data.codec);
--
--    let pagesOffset = +colChunk.meta_data.data_page_offset;
--    let pagesSize = +colChunk.meta_data.total_compressed_size;
--    let pagesBuf = await this.read(pagesOffset, pagesSize);
--
--    return decodeDataPages(pagesBuf, {
--      type: type,
--      rLevelMax: field.rLevelMax,
--      dLevelMax: field.dLevelMax,
--      compression: compression
--    });
end

function ParquetEnvelopeReader:readFooter()
  local trailerLen = #PARQUET_MAGIC + 4
  local trailerBuf = self.read(self.fileSize - trailerLen, trailerLen)
  
  if parquet_util.slice(trailerBuf, 5) ~= PARQUET_MAGIC then
    error('not a valid parquet file')
  end
  
  local metadataSize = vstruct.read('< u4', string.sub(trailerBuf, 1, 5))[1]
  
  local metadataOffset = self.fileSize - metadataSize - trailerLen
  if metadataOffset < #PARQUET_MAGIC then
    error('invalid metadata size')
  end
  
  local metadataBuf = self.read(metadataOffset, metadataSize)
  local metadata = parquet_thrift.FileMetaData:new()
  parquet_util.decodeThrift(metadata, metadataBuf)
  return metadata
end

return ParquetEnvelopeReader
