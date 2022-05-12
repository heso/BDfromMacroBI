api_url = 'https://api.macroserver.ru/analytics/goodbi/'

captions_Deals = ['id INTEGER',
                  'agreement_date DATE',
                  'area double precision',
                  'category TEXT',
                  'summ double precision',
                  'bank BOOLEAN',
                  'bank_name TEXT',
                  'deal_program TEXT',
                  'agent TEXT',
                  'mediator_comission double precision',
                  'id_house INTEGER',
                  'type_rus TEXT',
                  'complex_name TEXT',
                  'house_name TEXT']

captions_Leads = ['id INTEGER',
                  'date_added DATE',
                  'category TEXT',
                  'status TEXT',
                  'id_house INTEGER',
                  'type_rus TEXT',
                  'complex_name TEXT',
                  'house_name TEXT']

captions_Houses = ['houseID INTEGER',
                   'houseName TEXT',
                   'complexID INTEGER',
                   'complexName TEXT',
                   'houseAddress TEXT',
                   'houseStatus INTEGER']

captions_types_translations = ['type_eng TEXT',
                               'type_rus TEXT']

types_translations = [('flat', 'квартира'),
                      ('comm', 'коммерция'),
                      ('garage', 'парковка'),
                      ('land', 'земля'),
                      ('storageroom', 'кладовка'),
                      ('house', 'дом')]