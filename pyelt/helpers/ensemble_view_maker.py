from sample_domains.test_domain import *

patient_ensemble = [Patient, Handeling, PatientHandelingLink]

sql = ""
sql_from = ""
sql_where = ""
for cls in patient_ensemble:
    cls.init_cls()
    if not cls.__base__ == Link:
        hub_name = cls.get_hub_name()
        view_name = hub_name.replace('_hub', '_view')
        sql_from += view_name + ','
    else:
        sql_from += cls.get_name() + ','
        for prop_name, link_ref in cls.__dict__.items():
            if isinstance(link_ref, LinkReference) or isinstance(link_ref, DynamicLinkReference):
                if 'entity_cls' in link_ref.__dict__ and link_ref.entity_cls in patient_ensemble:
                    hub_name = link_ref.entity_cls.cls_get_hub_name()
                    view_name = hub_name.replace('_hub', '_view')
                    sql_where += """{} = {}._id AND """.format(link_ref.get_fk(), view_name)



sql = """SELECT * FROM {} WHERE {}""".format(sql_from, sql_where)
print(sql)


class Foo():
    """docstring"""
    bar = None

    def __init__(self):
        self.naam = 'ik ben wat'
        self.nummer = 123
        self.datum = ''


f = Foo()
for name, prop in f.__dict__.items():
    print(name, prop)
    if name == 'datum':
        f.__dict__[name] = 'nu ben ik ook wat'

print(f.__dict__)
print(f.__dict__.keys())
print(f.__dict__.items())
print(f.__dict__.get('naam'))
print(f.datum)
print(Foo.__dict__)

pipeline = get_global_test_pipeline()
pipe = pipeline.get_or_create_pipe('test_system', config=test_system_config)
pipe.register_domain(_domain_rob)
pipe.register_domain(_ensemble_views)
pipe.mappings = []
pipeline.run()

