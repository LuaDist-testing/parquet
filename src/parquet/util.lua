local M = {}

M.arrayClone = function(t)
  local r = {}
  for k,v in pairs(t) do r[k] = v end
  return r
end

M.arrayPush = function(t, value)
  if M.isArray(value) then
    for _, v in pairs(value) do t[#t+1] = v end
  else
    t[#t+1] = value
  end
  return t
end

M.fclose = function(file)
  file:close()
end

M.fopen = function(filePath)
  local file = assert(io.open(filePath, 'r'))
  return file
end

M.fread = function(file, position, length)
  file:seek('set', position)
  return file:read(length)
end

M.fsize = function(file)
  return file:seek('end')
end

M.isArray = function(t)
  if type(t) ~= 'table' then return false end
  local i = 0
  for _ in pairs(t) do
     i = i + 1
     if t[i] == nil then return false end
  end
  return true
end

M.iterator = function(t)
  local i = 0
  return function()
    i = i + 1
    return t[i]
  end
end

M.keyCount = function(t)
  local c = 0
  for _ in pairs(t) do c = c + 1 end
  return c
end

M.splice = function(t, start, deleteCount)
  deleteCount = deleteCount or 1
  for i=1,deleteCount do
    t[start+i-1] = nil
  end
  return t
end

M.split = function(s, sep)
  sep = sep or ','
  local fields = {}
  local pattern = string.format("([^%s]+)", sep)
  s:gsub(pattern, function(c) fields[#fields+1] = c end)
  return fields
end

M.slice = function(t, startIndex)
  if type(t) == 'table' then
    local r = {}
    for i=(startIndex or 1),#t do
      r[#r+1] = t[i]
    end
    return r
  else
    return string.sub(t, startIndex)
  end
end

return M
