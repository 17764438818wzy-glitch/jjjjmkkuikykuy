from article_repository.years import infer_year


def test_supported_student_number_shapes_use_four_year_offset():
    for number, expected in [('2018511999','2022'),('201905168042','2023'),
                             ('1910430039','2023'),('1901035','2023'),
                             ('2107212120','2025'),('F201902126035','2023'),('19041878','2023')]:
        assert infer_year({'folder_name':'某同学_'+number}) == expected


def test_original_filename_and_parent_path_are_fallbacks_only():
    assert infer_year({'folder_name':'原始文件夹未记录','files':[
        {'kind':'原文','name':'1901035 某同学 自动封口机设计.doc'}]}) == '2023'
    assert infer_year({'folder_name':'毕业设计说明书',
        'source_key':'folder:/archive/source/某同学201921176036毕业设计/说明书'}) == '2023'
    assert infer_year({'folder_name':'某同学_2018511999',
        'original_name':'其他-201905168042.docx'}) == '2022'


def test_dates_hashes_class_names_and_attachment_numbers_are_not_student_numbers():
    assert infer_year({'folder_name':'0123456789abcdef0123456789abcdef',
        'source_key':'folder:/runtime/uploads/20261018/0123456789abcdef0123456789abcdef',
        'files':[{'kind':'原文','name':'初稿20230418.docx'},
                 {'kind':'附件','name':'20230512132019.zip'}]}) is None
    assert infer_year({'folder_name':'2019级某专业'}) is None
    assert infer_year({'folder_name':'2018511999_201905168042'}) is None
    assert infer_year({'folder_name':'某同学_1106059'}) is None


def test_four_digit_year_before_2010_is_not_decoded_as_two_digits():
    assert infer_year({'folder_name':'学号2008511999'}) == '2012'
    assert infer_year({'folder_name':'学号1999511999'}) == '2003'


def test_compact_date_with_hour_is_not_an_implicit_student_number():
    assert infer_year({'folder_name':'稿件_2023041812'}) is None
    assert infer_year({'folder_name':'上传2022110501'}) is None
    assert infer_year({'folder_name':'学号201906111004'}) == '2023'


def test_confirmed_school_two_digit_encoding_has_priority_over_possible_four_digit_year():
    assert infer_year({'folder_name':'某同学_2002212164',
        'source_key':'folder:/总论文/保定学院/思想政治教育/某同学_2002212164'}) == '2024'


def test_short_admission_ids_and_two_digit_19_ids_do_not_look_like_historic_timestamps():
    assert infer_year({'folder_name':'某同学_1702042'}) == '2021'
    assert infer_year({'folder_name':'某同学_1801106'}) == '2022'
    assert infer_year({'folder_name':'某同学_1911031012'}) == '2023'
