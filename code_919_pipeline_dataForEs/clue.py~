# coding=utf-8
import math

from base import CategoricalTextMixIn, ClueFeature, FieldExistsMixIn, ParseNumberMixIn

import sys
reload(sys)
sys.setdefaultencoding('utf8')

class MainIndustry(CategoricalTextMixIn, ClueFeature):

    name = 'clueTable_main_industry'
    field_name = 'main_industry'
    vocabulary = {}
    vocabulary_size = 0


class Position(CategoricalTextMixIn, ClueFeature):

    name = 'clueTable_position'
    field_name = 'Clue_Entry_Major'
    vocabulary = {}
    vocabulary_size = 0



class Product(CategoricalTextMixIn, ClueFeature):

    name = 'clueTable_product'
    field_name = 'main_produce'
    vocabulary = {}
    vocabulary_size = 0


class CompanyType(CategoricalTextMixIn, ClueFeature):

    name = 'clueTable_company_type'
    field_name = 'com_type'
    vocabulary = {}
    vocabulary_size = 0



class MainMarket(CategoricalTextMixIn, ClueFeature):

    name = 'clueTable_main_market'
    field_name = 'main_market'
    vocabulary = {}
    vocabulary_size = 0


class Province(CategoricalTextMixIn, ClueFeature):

    name = 'clueTable_province'
    field_name = 'param4'
    vocabulary = {}
    vocabulary_size = 0


class City(CategoricalTextMixIn, ClueFeature):

    name = 'clueTable_city'
    field_name = 'param5'
    vocabulary = {}
    vocabulary_size = 0


class HighStatus(CategoricalTextMixIn, ClueFeature):

    name = 'clueTable_high_status'
    field_name = 'param1'
    vocabulary = {}
    vocabulary_size = 0



class HasQQ(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_qq'
    field_name = 'Clue_Entry_Qq'


class HasWeChat(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_wechat'
    field_name = 'Clue_Entry_Wechat'


class HasEmail(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_email'
    field_name = 'Clue_Entry_Email'


class HasAddress(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_address'
    field_name = 'Com_Address'


class HasBirthday(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_birthday'
    field_name = 'Clue_Entry_Birthday'


class HasTelephone(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_telephone'
    field_name = 'Clue_Entry_Telephone'


class HasFrontDesk(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_frontdesk'
    field_name = 'qiantai'


class HasFax(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_fax'
    field_name = 'chuanzhen'


class HasLegal(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_legal'
    field_name = 'legal_person'


class HasWebsite(FieldExistsMixIn, ClueFeature):

    name = 'clueTable_has_website'
    field_name = 'com_site'


class LogRevenue(ParseNumberMixIn, ClueFeature):

    name = 'clueTable_log_revenue'
    field_name = 'turnover'
    use_log = True


class LogRegisteredCapital(ParseNumberMixIn, ClueFeature):

    name = 'clueTable_log_registered_capital'
    field_name = 'registed_capital'
    use_log = True


class LogNumberOfEmployees(ParseNumberMixIn, ClueFeature):

    name = 'clueTable_log_number_of_employees'
    field_name = 'employees_num'
    use_log = True
    default = 1



class IsLegalRep(ClueFeature):

    name = 'clueTable_is_legal_rep'

    @classmethod
    def calculate(cls, clue_id):
        dic = cls.get_data(clue_id)
        return int( len(dic['Clue_Entry_Name']) > 0 and (dic['Clue_Entry_Name'] == dic['legal_person']))



class DownloadCount(ClueFeature):

    name = 'clueTable_download_count'

    @classmethod
    def calculate(cls, clue_id):
        return int(cls.get_data(clue_id)['downloadCount'])


class EmailUsesCorpDomain(ClueFeature):##???

    name = 'clueTable_email_uses_corp_domain'

    @classmethod
    def calculate(cls, clue_id):
        dic = cls.get_data(clue_id)
        email = dic['Clue_Entry_Email']
        if not email:
            return 0
        parts = email.split('@')
        if len(parts) != 2:
            return 0
        domain = parts[1]
        return int(domain in dic['com_site'])


def _print_dict(d):
    for k, v in d.items():
        print '    %s:%s' % (k.decode('utf-8'), v)


#收集单词
def test_categorical_texts():
    print 'Test CategoricalTextMixIn subclasses'
    clue_ids = ['0112BA47B2C04421B82E164744E75DD9',
                '04323D70B06544FE9187181D50B573B3']
    features = CategoricalTextMixIn.__subclasses__()



    for f in features:
        print f.name
        for cid in clue_ids:
            print '    ', cid, '_',f.get_data(cid)[f.field_name].decode('utf-8'), '_',f.calculate(cid) #？
        _print_dict(f.vocabulary)

    fnameList=[f.name for f in features];#print fnameList


#属性值是否存在
def test_has_fields():
    print 'Test HasXX'
    clue_id = '04323D70B06544FE9187181D50B573B3'
    features = FieldExistsMixIn.__subclasses__()
    for f in features:
        print '    ', f.name, f.get_data(clue_id)[f.field_name].decode('utf-8'), f.calculate(clue_id)


#将一些数值取对数
def test_parse_number_features():
    print 'Test parse number features'
    clue_ids = ['04323D70B06544FE9187181D50B573B3',
                '057CF9EAFB0D4F68AB22FBB4FD194461']
    features = ParseNumberMixIn.__subclasses__()
    for f in features:
        print f.name
        for cid in clue_ids:
            print '    ', cid, f.get_data(cid)[f.field_name].decode('utf-8'), f.calculate(cid)


#取出来数值型的特征
def test_run():
    print 'Test run remaining features'
    clue_ids = ['057CF9EAFB0D4F68AB22FBB4FD194461',
                '0000792F627F4490A0E011A99DEC57C0',
                '00008FD1B1CA4CB48F4D8DC450E64575']
    features = [LogDescriptionLength, IsLegalRep, DownloadCount, EmailUsesCorpDomain] #???
    for f in features:
        print f.name
        for cid in clue_ids:
            print '    ', cid, f.calculate(cid)


if __name__ == '__main__':
    test_categorical_texts()
    #test_has_fields()
    #test_parse_number_features()
    #test_run()
