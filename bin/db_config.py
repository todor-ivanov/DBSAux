import os
import getpass

oraUser = os.environ.get("ORACLE_USER", None)

oraDsn = os.environ.get("ORACLE_TNS", None)

oraPw = os.environ.get("ORACLE_PASSWORD", None)

oraOwner = os.environ.get("ORACLE_OWNER", None)

if oraUser is None:
    oraUser = input("Enter Oracle User: ")

if oraPw is None:
    oraPw = getpass.getpass("Enter password for Oracle user %s: " % oraUser)

if oraDsn is None:
    oraDsn = input("Enter Oracle database: ")

if oraOwner is None:
    oraOwner = input("Enter owner for Oracle database %s: " % oraDsn)
