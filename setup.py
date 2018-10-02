import sys
import setuptools

# ==================================================
# Package information
# ==================================================
NAME = 'Undine'
AUTHOR = 'Sungup Moon'
VERSION = '0.1.3'
EMAIL = 'sungup@me.com'
LICENSE = 'MIT'
DESCRIPTION = 'Undine, the CLI program automation package.'
URL = 'https://github.com/Sungup/Undine'
DOWNLOAD_URL = '{url}/archive/v{ver}.tar.gz'.format(url=URL, ver=VERSION)
KEYWORDS = ['python', 'automation', 'CLI']


LONG_DESCRIPTION = open('README.md', 'r').read()

ENTRY_POINTS = {
    'console_scripts': [
        'undine-cli=undine.client.main:main',
        'undine-svr=undine.server.main:main',
        'undine-setup=undine.setup.main:main',
        'undine-gen-conf=undine.setup.generator:main',
        'undine-db-init=undine.setup.database:main'
    ]
}

PACKAGES = setuptools.find_packages(
    exclude=['example', 'tmp', 'undine_cli.py', 'undine_svr.py']
)

INSTALL_REQUIRES = [
    'mysql-connector',
    'mysqlclient',
    'pika',
    'terminaltables'
]

TESTS_REQUIRE = ['nose']
EXTRAS_REQUIRE = {}

# ==================================================
# Utilities
# ==================================================
PY3 = 3 == sys.version_info[0]
PY37 = PY3 and 6 < sys.version_info[1]


if __name__ == '__main__':
    setuptools.setup(
        name=NAME,
        author=AUTHOR,
        version=VERSION,
        author_email=EMAIL,
        license=LICENSE,
        description=DESCRIPTION,
        url=URL,
        download_url=DOWNLOAD_URL,
        long_description=LONG_DESCRIPTION,
        extras_require=EXTRAS_REQUIRE,
        tests_require=TESTS_REQUIRE,
        install_requires=INSTALL_REQUIRES,
        packages=PACKAGES,
        include_package_data=True,
        zip_safe=False,
        entry_points=ENTRY_POINTS
    )
