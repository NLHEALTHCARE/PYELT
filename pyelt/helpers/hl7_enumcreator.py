import csv
import os

os.getcwd()
os.chdir('C:\\!ontwikkel\CLINICS-DWH2.0\data\HL7')

with open('hl7_value_sets.csv', newline='') as csvfile:
    hl7reader = csv.reader(csvfile, delimiter=';')

    class_names = []

    for row in hl7reader:
        if row[0] not in class_names:
            class_names.append(row[0])
    class_names.pop(0)

    for class_name in class_names:
        print('Class {}:'.format(class_name))
        csvfile.seek(0) #info: deze regel zet de csvfile reader terug naar regel 0, anders wordt de laatste regel gepakt omdat de csvreader hiervoor al een keer gerund is.
        for row in hl7reader:
            if row[0] == class_name:
                display_name = row[4]
                display_name = display_name.lower().replace('(','').replace(')','').replace('-','_').replace(' ','_')
                code = row[2]
                print("""    {} = '{}'""".format(display_name, code))
        print('')

Class RoleLinkType(Enum):
 """