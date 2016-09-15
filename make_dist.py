import datetime
import os
import subprocess
import zipfile

from versions import current_version, version_archive

import versions
global new_version

"""
zie https://packaging.python.org/distributing/

"""
def make_dist():
    github = 'https://github.com/NLHEALTHCARE/PYELT'
    branch = 'master'
    package_name = 'pyelt'

    #0. Eerst voeren we de unittests uit. Mocht er een fout zijn, dan wordt proces van package maken gestopt
    failures = 0
    # failures = os.system('nosetests')

    if failures > 0:
        print('KAN GEEN PACKAGE AANMAKEN. UNIT TESTS NIET GELDIG...')
        return

    #1. we maken nieuw auto versie nummer aan
    version_parts = versions.current_version[0].split('.')
    version_minor = int(version_parts[2])
    version_minor += 1
    version_parts[2] = str(version_minor)
    new_version = '.'.join(version_parts)
    current_version_number = new_version

    #2. voordat package kan worden gemaakt moet eerst alle code zijn gecommit
    cmd = 'git commit -m "package-build V{}"'.format(new_version)
    os.system('git add *'.format(new_version))
    os.system('git commit -m "package-build V{}"'.format(new_version))
    os.system("git push {} {}".format(github, branch))
    git_label = 'package-build V{0}'.format(new_version)
    os.system('git tag -a V{0} -m "{0}"'.format(new_version, git_label))
    os.system("git push --tags".format(github, branch))

    #6 nieuwe versie nummer bewaren samen met git-hash en label
    git_commit_number = subprocess.check_output(["git", "rev-parse", 'HEAD']).decode('ascii')
    git_commit_number = git_commit_number.replace('\n', '')
    username = os.getlogin()
    current_version = """('{}','{}','{}', '{}', '{}')""".format(new_version, username, git_label, git_commit_number, datetime.datetime.now())
    version_archive.append(current_version)

    f = open('versions.py', 'w')
    f.write("""current_version = {}\r\n""".format(current_version))
    f.write("""version_archive = [\r\n""")
    for item in version_archive:
        f.write("""    {}\n, """.format(item))
    f.write("""]\r\n""")
    f.close()
    print(git_commit_number)


    #3. run setup dist
    archive_format = 'zip'
    os.system("python setup.py sdist")

    #4. dummy configfiles in zip zetten
    arc_file_name = "dist/{}-{}.{}".format(package_name, new_version, archive_format)
    # zip_file = zipfile.ZipFile(arc_file_name, "a" )
    # #lijst met dummy configs
    # extra_files = ['test_package_make_hj/config.py', 'test_package_make_hj/dir2/test3_config.py']
    # for file_name in extra_files:
    #     # f = os.path.basename(f)
    #     arch_dest_file = '{}-{}/{}'. format(package_name, new_version, file_name)
    #     zip_file.write(file_name, arch_dest_file, compress_type=zipfile.ZIP_DEFLATED)
    # zip_file.close()

    #5 publiceren
    result = os.system("twine upload {}".format(arc_file_name))


if __name__ == '__main__':
    make_dist()