local class = require 'middleclass'

--[[
 * A parquet cursor is used to retrieve rows from a parquet file in order
--]]
local ParquetCursor = class('parquet.ParquetCursor')

--[[
 * Create a new parquet reader from the file metadata and an envelope reader.
 * It is usually not recommended to call this constructor directly except for
 * advanced and internal use cases. Consider using getCursor() on the
 * ParquetReader instead
--]]
function ParquetCursor:initialize(metadata, envelopeReader, schema, columnList)
  self.metadata = metadata
  self.envelopeReader = envelopeReader
  self.schema = schema
  self.columnList = columnList
  self.rowGroup = {}
  self.rowGroupIndex = 0
end

--[[
 * Retrieve the next row from the cursor. Returns a row or NULL if the end
 * of the file was reached
--]]
function ParquetCursor:next()
  error('not implemented yet')
  --if (this.rowGroup.length === 0) {
  --  if (this.rowGroupIndex >= this.metadata.row_groups.length) {
  --    return null
  --  }
  --
  --  let rowBuffer = await this.envelopeReader.readRowGroup(
  --      this.schema,
  --      this.metadata.row_groups[this.rowGroupIndex],
  --      this.columnList)
  --
  --  this.rowGroup = parquet_shredder.materializeRecords(this.schema, rowBuffer)
  --  this.rowGroupIndex++
  --}
  --
  --return this.rowGroup.shift()
end

--[[
 * Rewind the cursor the the beginning of the file
--]]
function ParquetCursor:rewind()
  self.rowGroup = {}
  self.rowGroupIndex = 0
end

return ParquetCursor
