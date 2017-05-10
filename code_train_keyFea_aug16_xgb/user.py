# coding=utf-8
import math

from base import CategoricalTextMixIn, UserFeature, FieldExistsMixIn, ParseNumberMixIn

class Major(CategoricalTextMixIn, UserFeature):

    name = 'userPortal_major'
    field_name = 'Major'
    vocabulary = {}
    vocabulary_size = 0

class CompanyName(CategoricalTextMixIn, UserFeature):

    name = 'userPortal_companyName'
    field_name = 'CompanyName'
    vocabulary = {}
    vocabulary_size = 0


class Profession(CategoricalTextMixIn, UserFeature):

    name = 'userPortal_profession'
    field_name = 'profession'
    vocabulary = {}
    vocabulary_size = 0


class Duty(CategoricalTextMixIn, UserFeature):

    name = 'userPortal_duty'
    field_name = 'duty'
    vocabulary = {}
    vocabulary_size = 0

"""
class UserType(CategoricalTextMixIn, UserFeature):

    name = 'user_type'
    field_name = 'User_Type' #?
    vocabulary = {}
    vocabulary_size = 0
"""

class Gender(CategoricalTextMixIn, UserFeature):

    name = 'userPortal_gender'
    field_name = 'User_Sex'
    vocabulary = {}
    vocabulary_size = 0
"""
class Constellation(CategoricalTextMixIn, UserFeature):

    name = 'constellation'
    field_name = 'User_Constellation'
    vocabulary = {}
    vocabulary_size = 0


class Hobby(CategoricalTextMixIn, UserFeature):

    name = 'hobby'
    field_name = 'User_Hobby'
    vocabulary = {}
    vocabulary_size = 0


class DataStatus(CategoricalTextMixIn, UserFeature):

    name = 'data_status'
    field_name = 'Data_Status'
    vocabulary = {}
    vocabulary_size = 0

class RegSourceType(CategoricalTextMixIn, UserFeature):

    name = 'reg_source_type'
    field_name = 'Reg_Source_Type'
    vocabulary = {}
    vocabulary_size = 0

class ChannelId(CategoricalTextMixIn, UserFeature):

    name = 'channel_id'
    field_name = 'channel_id'
    vocabulary = {}
    vocabulary_size = 0


class HasNick(FieldExistsMixIn, UserFeature):

    name = 'has_nickname'
    field_name = 'User_NickName'
"""
class HasTelephone(FieldExistsMixIn, UserFeature):

    name = 'userPortal_has_telephone'
    field_name = 'User_Telephone'


class HasQQ(FieldExistsMixIn, UserFeature):

    name = 'userPortal_has_qq'
    field_name = 'User_QQ'

class HasWeChat(FieldExistsMixIn, UserFeature):

    name = 'userPortal_has_wechat'
    field_name = 'User_Wechat'

class HasEmail(FieldExistsMixIn, UserFeature):

    name = 'userPortal_has_email'
    field_name = 'User_Email'

class HasAddress(FieldExistsMixIn, UserFeature):

    name = 'userPortal_has_address'
    field_name = 'User_Address'


class HasImg(FieldExistsMixIn, UserFeature):

    name = 'userPortal_has_img'
    field_name = 'User_Header_Img_Url'

class HasSite(FieldExistsMixIn, UserFeature):

    name = 'userPortal_has_site'
    field_name = 'neturl'

class HasTableHead(FieldExistsMixIn, UserFeature):

    name = 'userPortal_has_table_head'
    field_name = 'table_head'

"""
class LogUserIntegral(ParseNumberMixIn, UserFeature):

    name = 'log_user_integral'
    field_name = 'User_Integral'
    use_log = True

class LogUserAmount(ParseNumberMixIn, UserFeature):

    name = 'log_user_amount'
    field_name = 'User_Amount'
    use_log = True

class LogUserAge(ParseNumberMixIn, UserFeature):

    name = 'log_user_age'
    field_name = 'User_Age'
    use_log = True
"""


def _print_dict(d):
    for k, v in d.items():
        print '    %s:%s' % (k.decode('utf-8'), v)

def test_categorical_texts():
    print 'Test CategoricalTextMixIn subclasses'
    user_ids = ['0034D44E40C54288A4962D07788CE7EF',
                '002E76A4C7E541FBB659E83E1C389496'][:1]
    features = CategoricalTextMixIn.__subclasses__()
    user_feature = UserFeature.__subclasses__()
    features = [val for val in features if val in user_feature]
    print 'feature',features
    for f in features:
        print 'f name',f.name
        for cid in user_ids:
            print '    ', cid, f.get_data(cid)[f.field_name], f.calculate(cid)
        _print_dict(f.vocabulary)

def test_has_fields():
    print 'Test HasXX'
    clue_id = '0034D44E40C54288A4962D07788CE7EF'
    features = FieldExistsMixIn.__subclasses__()
    user_feature = UserFeature.__subclasses__()
    features = [val for val in features if val in user_feature]
    for f in features:
        print '    ', f.name, f.get_data(clue_id)[f.field_name], f.calculate(clue_id)

def test_parse_number_features():
    print 'Test parse number features'
    clue_ids = ['0034D44E40C54288A4962D07788CE7EF',
                '002E76A4C7E541FBB659E83E1C389496'][:1]
    features = ParseNumberMixIn.__subclasses__()
    user_feature = UserFeature.__subclasses__()
    features = [val for val in features if val in user_feature]
    for f in features:
        print f.name
        for cid in clue_ids:
            print '    ', cid, f.get_data(cid)[f.field_name], f.calculate(cid)



if __name__ == '__main__':
    test_categorical_texts()
    test_has_fields()
    test_parse_number_features()
