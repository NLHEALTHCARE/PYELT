import inspect

from sample_domains import entity_domain, role_domain, act_domain, participation_domain, test_domain
from pyelt.datalayers.database import Columns
from pyelt.datalayers.dv import DvEntity, Link, HybridSat, Sat

file_name = 'C:/temp/test4.gv'

module_colors = {
    'sample_domains.entity_domain': 'green',
    'sample_domains.role_domain': 'yellow',
    'sample_domains.act_domain': 'red',
    'sample_domains.participation_domain': 'blue',
    'sample_domains.test_domain': 'gray'
}

all_objects = []


# todo: overerving weergeven
# Done: linkname


def main():
    modules = [entity_domain, role_domain, act_domain, participation_domain]
    # modules = [test_domain]

    wiki_syntax = """===== DOMEIN =====\n\n"""
    wiki_syntax += """<graphviz neato svg 900x900>\n"""
    graph_syntax = make_main_graph(modules)
    wiki_syntax += graph_syntax
    wiki_syntax += """</graphviz>\n"""
    wiki_syntax += make_detail_graphs(modules)
    # make_file(doc)
    # make_svg()
    # open_browser()
    print(wiki_syntax)


def make_file(doc):
    file = open(file_name, 'w')
    file.write(doc)


def make_svg():
    import os
    cmd = 'dot -Tsvg {} > c:/temp/sample.svg'.format(file_name)
    os.system(cmd)


def open_browser():
    import os
    os.system('"c:\program files\internet explorer\iexplore.exe" file:///{}'.format('c:/temp/sample.svg'))


def make_main_graph(modules):
    graph_syntax = """digraph domain_model {
node [shape=box];
edge [arrowhead="none" fontsize=50];
splines=true;
sep="+25,25";
overlap=false;
nodesep=2;
node [fontsize=50];
"""
    graph_syntax = """digraph domain_model {
node [shape=box];
edge [arrowhead="none"];
overlap=false;
"""
    for module in modules:
        module_color = module_colors[module.__name__]
        for name, cls in inspect.getmembers(module, inspect.isclass):

            if cls.__base__ == DvEntity and cls.__module__ == module.__name__:
                graph_syntax += make_hub_graph(cls, module_color)
                graph_syntax = make_hub_graph_subclasses(cls, module_color, graph_syntax)
                # for sub_cls in cls.__subclasses__():
                #     graph_syntax += make_hub_graph_subclass(cls, sub_cls, module_color)
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__base__ == Link and cls.__module__ == module.__name__:
                graph_syntax += make_link_graph(cls, module_color)

    graph_syntax += """}\n\n"""
    return graph_syntax


def make_hub_graph_subclasses(cls, module_color, graph_syntax):
    subs = cls.__subclasses__()
    for sub_cls in cls.__subclasses__():
        graph_syntax += make_hub_graph_subclass(cls, sub_cls, module_color)
        graph_syntax = make_hub_graph_subclasses(sub_cls, module_color, graph_syntax)
    return graph_syntax


def clean_docstring(docstr):
    docstr = docstr.replace('\t', '')
    docstr = docstr.replace('    ', '')
    pos_link_start = docstr.find('http')
    while pos_link_start > 0:
        pos_link_end = docstr.find(' ', pos_link_start)
        if not pos_link_end:
            pos_link_end = docstr.find('\n', pos_link_start)
        docstr = '[[' + docstr[pos_link_start:pos_link_end] + '|' + docstr[pos_link_start:pos_link_end] + '|target]]'
        pos_link_start = docstr.find('http', pos_link_end)
    return docstr


def make_detail_graphs(modules):
    wiki_syntax = ''
    for module in modules:
        module_color = module_colors[module.__name__]
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__base__ == DvEntity and cls.__module__ == module.__name__:
                wiki_syntax += """\n===== {}_DETAILS =====\n\n""".format(cls.__name__.upper())
                wiki_syntax += make_doc_from_docstring(cls)
                wiki_syntax = make_doc_from_docstring_subclasses(cls, wiki_syntax)

                wiki_syntax += """<graphviz fdp svg 900x900>\n""".format(cls.__name__.upper())
                graph_syntax = """digraph """ + cls.__name__ + """ {
    node [shape=box];
    edge [arrowhead="none"];
    """
                graph_syntax += make_entity_graph(cls, module_color)
                graph_syntax = detail_graphs_subclasses(cls, module_color, graph_syntax)

                graph_syntax += """}\n"""
                wiki_syntax += graph_syntax
                wiki_syntax += """</graphviz>\n"""

    return wiki_syntax


def detail_graphs_subclasses(cls, module_color, graph_syntax):
    subs = cls.__subclasses__()
    for sub_cls in cls.__subclasses__():
        graph_syntax += make_sub_entity_graph(cls, sub_cls, module_color)
        graph_syntax = detail_graphs_subclasses(sub_cls, module_color, graph_syntax)
    return graph_syntax


def make_doc_from_docstring(cls):
    wiki_syntax = ''
    if cls.__doc__:
        wiki_syntax += "__**{}**__: ".format(cls.__name__)
        wiki_syntax += clean_docstring(cls.__doc__)
        wiki_syntax += '\n\n'
    sat_classes = cls.get_this_class_sats()
    for sat_cls in sat_classes.values():
        if sat_cls.__doc__:
            wiki_syntax += "__//{}//__: ".format(sat_cls.name)
            wiki_syntax += clean_docstring(sat_cls.__doc__)
            wiki_syntax += '\n\n'

    return wiki_syntax


def make_doc_from_docstring_subclasses(cls, wiki_syntax):
    subs = cls.__subclasses__()
    for sub_cls in cls.__subclasses__():
        wiki_syntax += make_doc_from_docstring(sub_cls)
        wiki_syntax = make_doc_from_docstring_subclasses(sub_cls, wiki_syntax)
    return wiki_syntax

def make_hub_graph(entity_cls: DvEntity, module_color='black'):
    entity_cls.init_cls()
    graph_syntax = ''
    hub_name = entity_cls.hub.name
    if hub_name not in all_objects:
        graph_syntax += """{0} [shape="box" penwidth="3" color="{2}" URL="#{1}_details" target="_parent" style="filled" gradientangle="270" fillcolor="white:aqua" ];\n\n""".format(
            hub_name, entity_cls.__name__.lower(), module_color)
        all_objects.append(hub_name)
    return graph_syntax


def make_hub_graph_subclass(base_cls, sub_cls, module_color):
    graph_syntax = ''

    base_name = base_cls.__name__.lower()
    hub_name = base_cls.hub.name
    if base_cls.__base__ == DvEntity:
        base_name = hub_name
    sub_hub_name = sub_cls.__name__.lower()
    if sub_hub_name not in all_objects:
        graph_syntax += """{0} [shape="box" penwidth="3" color="{2}" URL="#{1}_details" target="_parent" style="filled" gradientangle="270" fillcolor="white:aqua" ];\n""".format(
            sub_hub_name, hub_name.replace('_hub', ''), module_color)
        graph_syntax += """{0} -> {1} [arrowhead="onormal" arrowsize=8];\n""".format(
            sub_hub_name, base_name)
        all_objects.append(sub_hub_name)
    return graph_syntax

def make_entity_graph(entity_cls: DvEntity, module_color='black'):
    """Maak een subgraph van hub met sats eromheen """
    entity_cls.init_cls()
    graph_syntax = ''
    # doc += """beschrijving [shape="None" label = "{}"];\n""".format(doc_string)
    hub_name = entity_cls.hub.name
    # doc = 'subgraph cluster_' + entity_cls.__name__
    # doc += '{'
    # doc += """pos = "0,500!";"""
    graph_syntax += """{0} [shape="box"  penwidth="3" color="{1}" URL="#{0}" style="filled" gradientangle="270" fillcolor="white:aqua" ];\n""".format(hub_name, module_color)

    # sat_classes = entity_cls.get_sats()
    # for sat_cls in sat_classes.values():
    for key, sat_cls in entity_cls.__dict__.items():
        if inspect.isclass(sat_cls) and Sat in sat_cls.__mro__:
            sat_cls.init_cols()
            if sat_cls.__base__ == HybridSat:
                label = '<b>{} (hybrid)</b><br/>'.format(sat_cls.name)
                test = sat_cls.type
                label += '<br/>Types: {}<br/>'.format(sat_cls.get_types())
            else:
                label = '<b>{}</b><br/>'.format(sat_cls.name)
            columns = sat_cls.get_columns()
            for col in columns:
                if isinstance(col, Columns.RefColumn):
                    label += '<br/>{} (ref)'.format(col.name)
                else:
                    label += '<br/>{} ({})'.format(col.name, col.type)
            graph_syntax += """{0} [shape="box" penwidth="3" color="{2}" style="filled" gradientangle="270" fillcolor="white:yellow" label=<{1}>];\n""".format(sat_cls.name, label, module_color)

            graph_syntax += """{0} -> {1} [arrowhead="crow"];\n""".format(hub_name, sat_cls.name)

    graph_syntax += '\n\n'

    return graph_syntax


def make_sub_entity_graph(base_cls: DvEntity, sub_cls: DvEntity, module_color='black'):
    """Maak een subgraph van hub met sats eromheen """
    base_cls.init_cls()
    graph_syntax = ''
    # doc += """beschrijving [shape="None" label = "{}"];\n""".format(doc_string)
    base_name = base_cls.__name__.lower()
    hub_name = base_cls.hub.name
    if base_cls.__base__ == DvEntity:
        base_name = hub_name

    sub_ent_name = sub_cls.__name__.lower()
    # doc = 'subgraph cluster_' + entity_cls.__name__
    # doc += '{'
    # doc += """pos = "0,500!";"""
    graph_syntax += """{0} [shape="box"  penwidth="3" color="{1}" URL="#{0}" style="filled" gradientangle="270" fillcolor="white:aqua" ];\n""".format(sub_ent_name, module_color)
    graph_syntax += """{0} -> {1} [arrowhead="onormal"];\n""".format(sub_ent_name, base_name)
    # sat_classes = entity_cls.get_sats()
    # for sat_cls in sat_classes.values():
    for key, sat_cls in sub_cls.__dict__.items():
        if inspect.isclass(sat_cls) and Sat in sat_cls.__mro__:
            sat_cls.init_cols()
            if sat_cls.__base__ == HybridSat:
                label = '<b>{} (hybrid)</b><br/>'.format(sat_cls.name)
                test = sat_cls.type
                label += '<br/>Types: {}<br/>'.format(sat_cls.get_types())
            else:
                label = '<b>{}</b><br/>'.format(sat_cls.name)
            columns = sat_cls.get_columns()
            for col in columns:
                if isinstance(col, Columns.RefColumn):
                    label += '<br/>{} (ref)'.format(col.name)
                else:
                    label += '<br/>{} ({})'.format(col.name, col.type)
            graph_syntax += """{0} [shape="box" penwidth="3" color="{2}" style="filled" gradientangle="270" fillcolor="white:yellow" label=<{1}>];\n""".format(sat_cls.name, label, module_color)

            graph_syntax += """{0} -> {1} [arrowhead="crow"];\n""".format(sub_ent_name, sat_cls.name)

    graph_syntax += '\n\n'

    return graph_syntax

def make_link_graph(link_cls, module_color='black'):
    graph_syntax = ''

    if link_cls.__name__ not in all_objects:
        all_objects.append(link_cls.__name__)
        link_refs = link_cls.get_link_refs()
        graph_syntax += """{0} [shape="box" penwidth="3" color="{1}" URL="" style="filled" gradientangle="270" fillcolor="white:#FF9999"];\n""".format(link_cls.__name__, module_color)

        for name, link_ref in link_refs.items():
            if name.lower() != link_ref.entity_cls.__name__.lower():
                n = link_ref.entity_cls.__name__
                graph_syntax += """{} -> {} [label="{}"];\n""".format(link_ref.entity_cls.hub.name, link_cls.__name__, name)

            else:
                graph_syntax += """{} -> {};\n""".format(link_ref.entity_cls.hub.name, link_cls.__name__, )

    return graph_syntax


if __name__ == '__main__':
    main();
