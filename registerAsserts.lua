local say = require 'say'

local EPSILON_DEFAULT = 0.01

local registerAsserts = function(assert)

  -----------------------------------------------------------------------------
  say:set('assertion.equal_absTol.positive', 'Expected %s to equal %s within absolute tolerance %s')
  say:set('assertion.equal_absTol.negative', 'Expected %s to not equal %s within absolute tolerance %s')
  assert:register('assertion', 'equal_absTol', function(_, arguments)
    local a = arguments[1]
    local b = arguments[2]
    local epsilon = arguments[3] or EPSILON_DEFAULT
    if #a ~= #b then return false end
    for i=1,#a do
      if math.abs(a[i] - b[i]) >= epsilon then return false end
    end
    return true
  end, 'assertion.equal_absTol.positive', 'assertion.equal_absTol.negative')
  
end

return registerAsserts
