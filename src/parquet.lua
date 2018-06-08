local schema = require 'parquet.schema'
local shredder = require 'parquet.shred'

local M = {
  ParquetSchema = schema.ParquetSchema,
  ParquetShredder = shredder
}

return M
