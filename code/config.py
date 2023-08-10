from configparser import ConfigParser


def config(filename='E:\project\ETL_Pipeline\code\db.ini', section='default'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        section1 = params[0][1]
        if parser.has_section(section1):
            params2 = parser.items(section1)
            for p in params2:
                db[p[0]] = p[1]
            # print(params2)
          
            
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

config()
