# PHPPgAdmin Source-to-Image
You can use this repository to run PHPPgAdmin in your Openshift project using
Source-to-Image technology.

## Usage
Simply use the PHP Builder from the catalog. Set the PHP version to 7.

If the builder is not in your catalog, ask your cluster admin to add it:
https://github.com/sclorg/s2i-php-container

## Environment variables
```
# MENDATORY
# pecify the PostgreSQL server hostname in your Openshift project.
HOST=postgresql

# OPTIONNAL
# Activate extra security, like denying login without password or with postgres user.
# Default: true
EXTRA_LOGIN_SECURITY=true
```

## References
The PHPPgAdmin files come from the https://github.com/halojoy/phppgadmin-for-PHP7
repository so it's compatible with PHP 7.
