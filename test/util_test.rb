require File.dirname(__FILE__) + '/test_helper'

class UtilTest < Test::Unit::TestCase

  def test_ability_prefix_name_builder
    assert_equal(Gearman::Util.ability_name_with_prefix("test","a"),"test\ta")
  end

  def test_ability_name_for_perl
    assert_equal(Gearman::Util.ability_name_for_perl("test","a"),"test\ta")
  end
end
