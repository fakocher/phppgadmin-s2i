<?php

/**
 * A Class that implements the DB Interface for Postgres
 * Note: This Class uses ADODB and returns RecordSets.
 *
 * $Id: Postgres.php,v 1.320 2008/02/20 20:43:09 ioguix Exp $
 */

include_once('./classes/database/ADODB_base.php');

class Postgres extends ADODB_base {

	var $major_version = 12;
	// Max object name length
	var $_maxNameLen = 63;
	// Store the current schema
	var $_schema;
	// Map of database encoding names to HTTP encoding names.  If a
	// database encoding does not appear in this list, then its HTTP
	// encoding name is the same as its database encoding name.
	var $codemap = array(
		'BIG5' => 'BIG5',
		'EUC_CN' => 'GB2312',
		'EUC_JP' => 'EUC-JP',
		'EUC_KR' => 'EUC-KR',
		'EUC_TW' => 'EUC-TW',
		'GB18030' => 'GB18030',
		'GBK' => 'GB2312',
		'ISO_8859_5' => 'ISO-8859-5',
		'ISO_8859_6' => 'ISO-8859-6',
		'ISO_8859_7' => 'ISO-8859-7',
		'ISO_8859_8' => 'ISO-8859-8',
		'JOHAB' => 'CP1361',
		'KOI8' => 'KOI8-R',
		'LATIN1' => 'ISO-8859-1',
		'LATIN2' => 'ISO-8859-2',
		'LATIN3' => 'ISO-8859-3',
		'LATIN4' => 'ISO-8859-4',
		'LATIN5' => 'ISO-8859-9',
		'LATIN6' => 'ISO-8859-10',
		'LATIN7' => 'ISO-8859-13',
		'LATIN8' => 'ISO-8859-14',
		'LATIN9' => 'ISO-8859-15',
		'LATIN10' => 'ISO-8859-16',
		'SJIS' => 'SHIFT_JIS',
		'SQL_ASCII' => 'US-ASCII',
		'UHC' => 'WIN949',
		'UTF8' => 'UTF-8',
		'WIN866' => 'CP866',
		'WIN874' => 'CP874',
		'WIN1250' => 'CP1250',
		'WIN1251' => 'CP1251',
		'WIN1252' => 'CP1252',
		'WIN1256' => 'CP1256',
		'WIN1258' => 'CP1258'
	);
	var $defaultprops = array('', '', '');
	// Extra "magic" types.  BIGSERIAL was added in PostgreSQL 7.2.
	var $extraTypes = array('SERIAL', 'BIGSERIAL');
	// Foreign key stuff.  First element MUST be the default.
	var $fkactions = array('NO ACTION', 'RESTRICT', 'CASCADE', 'SET NULL', 'SET DEFAULT');
	var $fkdeferrable = array('NOT DEFERRABLE', 'DEFERRABLE');
	var $fkinitial = array('INITIALLY IMMEDIATE', 'INITIALLY DEFERRED');
	var $fkmatches = array('MATCH SIMPLE', 'MATCH FULL');
	// Function properties
	var $funcprops = array( array('', 'VOLATILE', 'IMMUTABLE', 'STABLE'),
							array('', 'CALLED ON NULL INPUT', 'RETURNS NULL ON NULL INPUT'),
							array('', 'SECURITY INVOKER', 'SECURITY DEFINER'));
	// Default help URL
	var $help_base;
	// Help sub pages
	var $help_page;
	// Name of id column
	var $id = 'oid';
	// Supported join operations for use with view wizard
	var $joinOps = array('INNER JOIN' => 'INNER JOIN', 'LEFT JOIN' => 'LEFT JOIN', 'RIGHT JOIN' => 'RIGHT JOIN', 'FULL JOIN' => 'FULL JOIN');
	// Map of internal language name to syntax highlighting name
	var $langmap = array(
		'sql' => 'SQL',
		'plpgsql' => 'SQL',
		'php' => 'PHP',
		'phpu' => 'PHP',
		'plphp' => 'PHP',
		'plphpu' => 'PHP',
		'perl' => 'Perl',
		'perlu' => 'Perl',
		'plperl' => 'Perl',
		'plperlu' => 'Perl',
		'java' => 'Java',
		'javau' => 'Java',
		'pljava' => 'Java',
		'pljavau' => 'Java',
		'plj' => 'Java',
		'plju' => 'Java',
		'python' => 'Python',
		'pythonu' => 'Python',
		'plpython' => 'Python',
		'plpythonu' => 'Python',
		'ruby' => 'Ruby',
		'rubyu' => 'Ruby',
		'plruby' => 'Ruby',
		'plrubyu' => 'Ruby'
	);
	// Predefined size types
	var $predefined_size_types = array('abstime','aclitem','bigserial','boolean','bytea','cid','cidr','circle','date','float4','float8','gtsvector','inet','int2','int4','int8','macaddr','money','oid','path','polygon','refcursor','regclass','regoper','regoperator','regproc','regprocedure','regtype','reltime','serial','smgr','text','tid','tinterval','tsquery','tsvector','varbit','void','xid');
	// List of all legal privileges that can be applied to different types
	// of objects.
	var $privlist = array(
  		'table' => array('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'REFERENCES', 'TRIGGER', 'ALL PRIVILEGES'),
  		'view' => array('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'REFERENCES', 'TRIGGER', 'ALL PRIVILEGES'),
  		'sequence' => array('SELECT', 'UPDATE', 'ALL PRIVILEGES'),
  		'database' => array('CREATE', 'TEMPORARY', 'CONNECT', 'ALL PRIVILEGES'),
  		'function' => array('EXECUTE', 'ALL PRIVILEGES'),
  		'language' => array('USAGE', 'ALL PRIVILEGES'),
  		'schema' => array('CREATE', 'USAGE', 'ALL PRIVILEGES'),
  		'tablespace' => array('CREATE', 'ALL PRIVILEGES'),
		'column' => array('SELECT', 'INSERT', 'UPDATE', 'REFERENCES','ALL PRIVILEGES')
	);
	// List of characters in acl lists and the privileges they
	// refer to.
	var $privmap = array(
		'r' => 'SELECT',
		'w' => 'UPDATE',
		'a' => 'INSERT',
  		'd' => 'DELETE',
		'D' => 'TRUNCATE',
  		'R' => 'RULE',
  		'x' => 'REFERENCES',
  		't' => 'TRIGGER',
  		'X' => 'EXECUTE',
  		'U' => 'USAGE',
  		'C' => 'CREATE',
  		'T' => 'TEMPORARY',
  		'c' => 'CONNECT'
	);
	// Rule action types
	var $rule_events = array('SELECT', 'INSERT', 'UPDATE', 'DELETE');
	// Select operators
	var $selectOps = array('=' => 'i', '!=' => 'i', '<' => 'i', '>' => 'i', '<=' => 'i', '>=' => 'i',
		'<<' => 'i', '>>' => 'i', '<<=' => 'i', '>>=' => 'i',
		'LIKE' => 'i', 'NOT LIKE' => 'i', 'ILIKE' => 'i', 'NOT ILIKE' => 'i', 'SIMILAR TO' => 'i',
		'NOT SIMILAR TO' => 'i', '~' => 'i', '!~' => 'i', '~*' => 'i', '!~*' => 'i',
		'IS NULL' => 'p', 'IS NOT NULL' => 'p', 'IN' => 'x', 'NOT IN' => 'x',
		'@@' => 'i', '@@@' => 'i', '@>' => 'i', '<@' => 'i',
		'@@ to_tsquery' => 't', '@@@ to_tsquery' => 't', '@> to_tsquery' => 't', '<@ to_tsquery' => 't',
		'@@ plainto_tsquery' => 't', '@@@ plainto_tsquery' => 't', '@> plainto_tsquery' => 't', '<@ plainto_tsquery' => 't');
	// Array of allowed trigger events
	var $triggerEvents= array('INSERT', 'UPDATE', 'DELETE', 'INSERT OR UPDATE', 'INSERT OR DELETE',
		'DELETE OR UPDATE', 'INSERT OR DELETE OR UPDATE');
	// When to execute the trigger
	var $triggerExecTimes = array('BEFORE', 'AFTER');
	// How often to execute the trigger
	var $triggerFrequency = array('ROW','STATEMENT');
	// Array of allowed type alignments
	var $typAligns = array('char', 'int2', 'int4', 'double');
	// The default type alignment
	var $typAlignDef = 'int4';
	// Default index type
	var $typIndexDef = 'BTREE';
	// Array of allowed index types
	var $typIndexes = array('BTREE', 'RTREE', 'GIST', 'GIN', 'HASH');
	// Array of allowed type storage attributes
	var $typStorages = array('plain', 'external', 'extended', 'main');
	// The default type storage
	var $typStorageDef = 'plain';

	/**
	 * Constructor
	 * @param $conn The database connection
	 */
	function Postgres($conn) {
		$this->ADODB_base($conn);
	}

	// Formatting functions

	/**
	 * Cleans (escapes) a string
	 * @param $str The string to clean, by reference
	 * @return The cleaned string
	 */
	function clean(&$str) {
		if ($str === null) return null;
		$str = str_replace("\r\n","\n",$str);
		$str = pg_escape_string($str);
		return $str;
	}

	/**
	 * Cleans (escapes) an object name (eg. table, field)
	 * @param $str The string to clean, by reference
	 * @return The cleaned string
	 */
	function fieldClean(&$str) {
		if ($str === null) return null;
		$str = str_replace('"', '""', $str);
		return $str;
	}

	/**
	 * Cleans (escapes) an array of field names
	 * @param $arr The array to clean, by reference
	 * @return The cleaned array
	 */
	function fieldArrayClean(&$arr) {
		foreach ($arr as $k => $v) {
			if ($v === null) continue;
			$arr[$k] = str_replace('"', '""', $v);
		}
		return $arr;
	}

	/**
	 * Cleans (escapes) an array
	 * @param $arr The array to clean, by reference
	 * @return The cleaned array
	 */
	function arrayClean(&$arr) {
		foreach ($arr as $k => $v) {
			if ($v === null) continue;
			$arr[$k] = pg_escape_string($v);
		}
		return $arr;
	}

	/**
	 * Escapes bytea data for display on the screen
	 * @param $data The bytea data
	 * @return Data formatted for on-screen display
	 */
	function escapeBytea($data) {
		return htmlentities($data, ENT_QUOTES, 'UTF-8');
	}

	/**
	 * Outputs the HTML code for a particular field
	 * @param $name The name to give the field
	 * @param $value The value of the field.  Note this could be 'numeric(7,2)' sort of thing...
	 * @param $type The database type of the field
	 * @param $extras An array of attributes name as key and attributes' values as value
	 */
	function printField($name, $value, $type, $extras = array()) {
		global $lang;

		// Determine actions string
		$extra_str = '';
		foreach ($extras as $k => $v) {
			$extra_str .= " {$k}=\"" . htmlspecialchars($v) . "\"";
		}

		switch (substr($type,0,9)) {
			case 'bool':
			case 'boolean':
				if ($value !== null && $value == '') $value = null;
				elseif ($value == 'true') $value = 't';
				elseif ($value == 'false') $value = 'f';

				// If value is null, 't' or 'f'...
				if ($value === null || $value == 't' || $value == 'f') {
					echo "<select name=\"", htmlspecialchars($name), "\"{$extra_str}>\n";
					echo "<option value=\"\"", ($value === null) ? ' selected="selected"' : '', "></option>\n";
					echo "<option value=\"t\"", ($value == 't') ? ' selected="selected"' : '', ">{$lang['strtrue']}</option>\n";
					echo "<option value=\"f\"", ($value == 'f') ? ' selected="selected"' : '', ">{$lang['strfalse']}</option>\n";
					echo "</select>\n";
				}
				else {
					echo "<input name=\"", htmlspecialchars($name), "\" value=\"", htmlspecialchars($value), "\" size=\"35\"{$extra_str} />\n";
				}
				break;
			case 'bytea':
			case 'bytea[]':
                if (!is_null($value)) {
				    $value = $this->escapeBytea($value);
                }
			case 'text':
			case 'text[]':
			case 'json':
			case 'jsonb': 
			case 'xml':
			case 'xml[]':
				$n = substr_count($value, "\n");
				$n = $n < 5 ? 5 : $n;
				$n = $n > 20 ? 20 : $n;
				echo "<textarea name=\"", htmlspecialchars($name), "\" rows=\"{$n}\" cols=\"75\"{$extra_str}>\n";
				echo htmlspecialchars($value);
				echo "</textarea>\n";
				break;
			case 'character':
			case 'character[]':
				$n = substr_count($value, "\n");
				$n = $n < 5 ? 5 : $n;
				$n = $n > 20 ? 20 : $n;
				echo "<textarea name=\"", htmlspecialchars($name), "\" rows=\"{$n}\" cols=\"35\"{$extra_str}>\n";
				echo htmlspecialchars($value);
				echo "</textarea>\n";
				break;
			default:
				echo "<input name=\"", htmlspecialchars($name), "\" value=\"", htmlspecialchars($value), "\" size=\"35\"{$extra_str} />\n";
				break;
		}
	}

	/**
	 * Formats a value or expression for sql purposes
	 * @param $type The type of the field
	 * @param $format VALUE or EXPRESSION
	 * @param $value The actual value entered in the field.  Can be NULL
	 * @return The suitably quoted and escaped value.
	 */
	function formatValue($type, $format, $value) {
		switch ($type) {
			case 'bool':
			case 'boolean':
				if ($value == 't')
					return 'TRUE';
				elseif ($value == 'f')
					return 'FALSE';
				elseif ($value == '')
					return 'NULL';
				else
					return $value;
				break;
			default:
				// Checking variable fields is difficult as there might be a size
				// attribute...
				if (strpos($type, 'time') === 0) {
					// Assume it's one of the time types...
					if ($value == '') return "''";
					elseif (strcasecmp($value, 'CURRENT_TIMESTAMP') == 0
							|| strcasecmp($value, 'CURRENT_TIME') == 0
							|| strcasecmp($value, 'CURRENT_DATE') == 0
							|| strcasecmp($value, 'LOCALTIME') == 0
							|| strcasecmp($value, 'LOCALTIMESTAMP') == 0) {
						return $value;
					}
					elseif ($format == 'EXPRESSION')
						return $value;
					else {
						$this->clean($value);
						return "'{$value}'";
					}
				}
				else {
					if ($format == 'VALUE') {
						$this->clean($value);
						return "'{$value}'";
					}
					return $value;
				}
		}
	}

	/**
	 * Formats a type correctly for display.  Postgres 7.0 had no 'format_type'
	 * built-in function, and hence we need to do it manually.
	 * @param $typname The name of the type
	 * @param $typmod The contents of the typmod field
	 */
	function formatType($typname, $typmod) {
		// This is a specific constant in the 7.0 source
		$varhdrsz = 4;

		// If the first character is an underscore, it's an array type
		$is_array = false;
		if (substr($typname, 0, 1) == '_') {
			$is_array = true;
			$typname = substr($typname, 1);
		}

		// Show lengths on bpchar and varchar
		if ($typname == 'bpchar') {
			$len = $typmod - $varhdrsz;
			$temp = 'character';
			if ($len > 1)
				$temp .= "({$len})";
		}
		elseif ($typname == 'varchar') {
			$temp = 'character varying';
			if ($typmod != -1)
				$temp .= "(" . ($typmod - $varhdrsz) . ")";
		}
		elseif ($typname == 'numeric') {
			$temp = 'numeric';
			if ($typmod != -1) {
				$tmp_typmod = $typmod - $varhdrsz;
				$precision = ($tmp_typmod >> 16) & 0xffff;
				$scale = $tmp_typmod & 0xffff;
				$temp .= "({$precision}, {$scale})";
			}
		}
		else $temp = $typname;

		// Add array qualifier if it's an array
		if ($is_array) $temp .= '[]';

		return $temp;
	}

	// Help functions

	/**
	 * Fetch a URL (or array of URLs) for a given help page.
	 */
	function getHelp($help) {
		$this->getHelpPages();

		if (isset($this->help_page[$help])) {
			if (is_array($this->help_page[$help])) {
				$urls = array();
				foreach ($this->help_page[$help] as $link) {
					$urls[] = $this->help_base . $link;
				}
				return $urls;
			} else
				return $this->help_base . $this->help_page[$help];
		} else
			return null;
	}

	function getHelpPages() {
		include_once('./help/PostgresDoc95.php');
		return $this->help_page;
	}

	// Database functions

	/**
	 * Return all information about a particular database
	 * @param $database The name of the database to retrieve
	 * @return The database info
	 */
	function getDatabase($database) {
		$this->clean($database);
		$sql = "SELECT * FROM pg_database WHERE datname='{$database}'";
		return $this->selectSet($sql);
	}

	/**
	 * Return all database available on the server
	 * @param $currentdatabase database name that should be on top of the resultset
	 * 
	 * @return A list of databases, sorted alphabetically
	 */
	function getDatabases($currentdatabase = NULL) {
		global $conf, $misc;

		$server_info = $misc->getServerInfo();

		if (isset($conf['owned_only']) && $conf['owned_only'] && !$this->isSuperUser()) {
			$username = $server_info['username'];
			$this->clean($username);
			$clause = " AND pr.rolname='{$username}'";
		}
		else $clause = '';

		if ($currentdatabase != NULL) {
			$this->clean($currentdatabase);
			$orderby = "ORDER BY pdb.datname = '{$currentdatabase}' DESC, pdb.datname";
		} 
		else
			$orderby = "ORDER BY pdb.datname";

		if (!$conf['show_system'])
			$where = ' AND NOT pdb.datistemplate';
		else
			$where = ' AND pdb.datallowconn';

		$sql = "
			SELECT pdb.datname AS datname, pr.rolname AS datowner, pg_encoding_to_char(encoding) AS datencoding,
				(SELECT description FROM pg_catalog.pg_shdescription pd WHERE pdb.oid=pd.objoid AND pd.classoid='pg_database'::regclass) AS datcomment,
				(SELECT spcname FROM pg_catalog.pg_tablespace pt WHERE pt.oid=pdb.dattablespace) AS tablespace,
				CASE WHEN pg_catalog.has_database_privilege(current_user, pdb.oid, 'CONNECT') 
					THEN pg_catalog.pg_database_size(pdb.oid) 
					ELSE -1 -- set this magic value, which we will convert to no access later  
				END as dbsize, pdb.datcollate, pdb.datctype
			FROM pg_catalog.pg_database pdb
				LEFT JOIN pg_catalog.pg_roles pr ON (pdb.datdba = pr.oid)
			WHERE true
				{$where}
				{$clause}
			{$orderby}";

		return $this->selectSet($sql);
	}

	/**
	 * Return the database comment of a db from the shared description table
	 * @param string $database the name of the database to get the comment for
	 * @return recordset of the db comment info
	 */
	function getDatabaseComment($database) {
		$this->clean($database);
		$sql = "SELECT description FROM pg_catalog.pg_database JOIN pg_catalog.pg_shdescription ON (oid=objoid AND classoid='pg_database'::regclass) WHERE pg_database.datname = '{$database}' ";
		return $this->selectSet($sql);
	}

	/**
	 * Return the database owner of a db
	 * @param string $database the name of the database to get the owner for
	 * @return recordset of the db owner info
	 */
	function getDatabaseOwner($database) {
		$this->clean($database);
		$sql = "SELECT usename FROM pg_user, pg_database WHERE pg_user.usesysid = pg_database.datdba AND pg_database.datname = '{$database}' ";
		return $this->selectSet($sql);
	}

	/**
	 * Returns the current database encoding
	 * @return The encoding.  eg. SQL_ASCII, UTF-8, etc.
	 */
	function getDatabaseEncoding() {
		return pg_parameter_status($this->conn->_connectionID, 'server_encoding');
	}

	/**
	 * Returns the current default_with_oids setting
	 * @return default_with_oids setting
	 */
	function getDefaultWithOid() {

		$sql = "SHOW default_with_oids";

		return $this->selectField($sql, 'default_with_oids');
	}

	/**
	 * Creates a database
	 * @param $database The name of the database to create
	 * @param $encoding Encoding of the database
	 * @param $tablespace (optional) The tablespace name
	 * @return 0 success
	 * @return -1 tablespace error
	 * @return -2 comment error
	 */
	function createDatabase($database, $encoding, $tablespace = '', $comment = '', $template = 'template1',
		$lc_collate = '', $lc_ctype = '')
	{
		$this->fieldClean($database);
		$this->clean($encoding);
		$this->fieldClean($tablespace);
		$this->fieldClean($template);
		$this->clean($lc_collate);
		$this->clean($lc_ctype);

		$sql = "CREATE DATABASE \"{$database}\" WITH TEMPLATE=\"{$template}\"";

		if ($encoding != '') $sql .= " ENCODING='{$encoding}'";
		if ($lc_collate != '') $sql .= " LC_COLLATE='{$lc_collate}'";
		if ($lc_ctype != '') $sql .= " LC_CTYPE='{$lc_ctype}'";

		if ($tablespace != '' && $this->hasTablespaces()) $sql .= " TABLESPACE \"{$tablespace}\"";

		$status = $this->execute($sql);
		if ($status != 0) return -1;

		if ($comment != '' && $this->hasSharedComments()) {
			$status = $this->setComment('DATABASE',$database,'',$comment);
			if ($status != 0) return -2;
		}

		return 0;
	}

	/**
	 * Renames a database, note that this operation cannot be
	 * performed on a database that is currently being connected to
	 * @param string $oldName name of database to rename
	 * @param string $newName new name of database
	 * @return int 0 on success
	 */
	function alterDatabaseRename($oldName, $newName) {
		$this->fieldClean($oldName);
		$this->fieldClean($newName);

		if ($oldName != $newName) {
			$sql = "ALTER DATABASE \"{$oldName}\" RENAME TO \"{$newName}\"";
			return $this->execute($sql);
		}
		else //just return success, we're not going to do anything
			return 0;
	}

	/**
	 * Drops a database
	 * @param $database The name of the database to drop
	 * @return 0 success
	 */
	function dropDatabase($database) {
		$this->fieldClean($database);
		$sql = "DROP DATABASE \"{$database}\"";
		return $this->execute($sql);
	}

	/**
	 * Changes ownership of a database
	 * This can only be done by a superuser or the owner of the database
	 * @param string $dbName database to change ownership of
	 * @param string $newOwner user that will own the database
	 * @return int 0 on success
	 */
	function alterDatabaseOwner($dbName, $newOwner) {
		$this->fieldClean($dbName);
		$this->fieldClean($newOwner);

		$sql = "ALTER DATABASE \"{$dbName}\" OWNER TO \"{$newOwner}\"";
		return $this->execute($sql);
	}

	/**
	 * Alters a database
	 * the multiple return vals are for postgres 8+ which support more functionality in alter database
	 * @param $dbName The name of the database
	 * @param $newName new name for the database
	 * @param $newOwner The new owner for the database
	 * @return 0 success
	 * @return -1 transaction error
	 * @return -2 owner error
	 * @return -3 rename error
	 * @return -4 comment error
	 */
	function alterDatabase($dbName, $newName, $newOwner = '', $comment = '') {

		$status = $this->beginTransaction();
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		if ($dbName != $newName) {
			$status = $this->alterDatabaseRename($dbName, $newName);
			if ($status != 0) {
				$this->rollbackTransaction();
				return -3;
			}
			$dbName = $newName;
		}

		if ($newOwner != '') {
			$status = $this->alterDatabaseOwner($newName, $newOwner);
			if ($status != 0) {
				$this->rollbackTransaction();
				return -2;
			}
		}
		
		$this->fieldClean($dbName);
		$status = $this->setComment('DATABASE', $dbName, '', $comment);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -4;
		}
		return $this->endTransaction();
	}

	/**
	 * Returns prepared transactions information
	 * @param $database (optional) Find only prepared transactions executed in a specific database
	 * @return A recordset
	 */
	function getPreparedXacts($database = null) {
		if ($database === null)
			$sql = "SELECT * FROM pg_prepared_xacts";
		else {
			$this->clean($database);
			$sql = "SELECT transaction, gid, prepared, owner FROM pg_prepared_xacts
				WHERE database='{$database}' ORDER BY owner";
		}

		return $this->selectSet($sql);
	}

	/**
	 * Searches all system catalogs to find objects that match a certain name.
	 * @param $term The search term
	 * @param $filter The object type to restrict to ('' means no restriction)
	 * @return A recordset
	 */
	function findObject($term, $filter) {
		global $conf;

		/*about escaping:
		 * SET standard_conforming_string is not available before 8.2
		 * So we must use PostgreSQL specific notation :/
		 * E'' notation is not available before 8.1
		 * $$ is available since 8.0
		 * Nothing specific from 7.4
		 **/

		// Escape search term for ILIKE match
		$this->clean($term);
		$this->clean($filter);
		$term = str_replace('_', '\_', $term);
		$term = str_replace('%', '\%', $term);

		// Exclude system relations if necessary
		if (!$conf['show_system']) {
			// XXX: The mention of information_schema here is in the wrong place, but
			// it's the quickest fix to exclude the info schema from 7.4
			$where = " AND pn.nspname NOT LIKE \$_PATERN_\$pg\_%\$_PATERN_\$ AND pn.nspname != 'information_schema'";
			$lan_where = "AND pl.lanispl";
		}
		else {
			$where = '';
			$lan_where = '';
		}

		// Apply outer filter
		$sql = '';
		if ($filter != '') {
			$sql = "SELECT * FROM (";
		}

		$term = "\$_PATERN_\$%{$term}%\$_PATERN_\$";

		$sql .= "
			SELECT 'SCHEMA' AS type, oid, NULL AS schemaname, NULL AS relname, nspname AS name
				FROM pg_catalog.pg_namespace pn WHERE nspname ILIKE {$term} {$where}
			UNION ALL
			SELECT CASE WHEN relkind='r' THEN 'TABLE' WHEN relkind='v' THEN 'VIEW' WHEN relkind='S' THEN 'SEQUENCE' END, pc.oid,
				pn.nspname, NULL, pc.relname FROM pg_catalog.pg_class pc, pg_catalog.pg_namespace pn
				WHERE pc.relnamespace=pn.oid AND relkind IN ('r', 'v', 'S') AND relname ILIKE {$term} {$where}
			UNION ALL
			SELECT CASE WHEN pc.relkind='r' THEN 'COLUMNTABLE' ELSE 'COLUMNVIEW' END, NULL, pn.nspname, pc.relname, pa.attname FROM pg_catalog.pg_class pc, pg_catalog.pg_namespace pn,
				pg_catalog.pg_attribute pa WHERE pc.relnamespace=pn.oid AND pc.oid=pa.attrelid
				AND pa.attname ILIKE {$term} AND pa.attnum > 0 AND NOT pa.attisdropped AND pc.relkind IN ('r', 'v') {$where}
			UNION ALL
			SELECT 'FUNCTION', pp.oid, pn.nspname, NULL, pp.proname || '(' || pg_catalog.oidvectortypes(pp.proargtypes) || ')' FROM pg_catalog.pg_proc pp, pg_catalog.pg_namespace pn
				WHERE pp.pronamespace=pn.oid AND NOT pp.proisagg AND pp.proname ILIKE {$term} {$where}
			UNION ALL
			SELECT 'INDEX', NULL, pn.nspname, pc.relname, pc2.relname FROM pg_catalog.pg_class pc, pg_catalog.pg_namespace pn,
				pg_catalog.pg_index pi, pg_catalog.pg_class pc2 WHERE pc.relnamespace=pn.oid AND pc.oid=pi.indrelid
				AND pi.indexrelid=pc2.oid
				AND NOT EXISTS (
					SELECT 1 FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_constraint c
					ON (d.refclassid = c.tableoid AND d.refobjid = c.oid)
					WHERE d.classid = pc2.tableoid AND d.objid = pc2.oid AND d.deptype = 'i' AND c.contype IN ('u', 'p')
				)
				AND pc2.relname ILIKE {$term} {$where}
			UNION ALL
			SELECT 'CONSTRAINTTABLE', NULL, pn.nspname, pc.relname, pc2.conname FROM pg_catalog.pg_class pc, pg_catalog.pg_namespace pn,
				pg_catalog.pg_constraint pc2 WHERE pc.relnamespace=pn.oid AND pc.oid=pc2.conrelid AND pc2.conrelid != 0
				AND CASE WHEN pc2.contype IN ('f', 'c') THEN TRUE ELSE NOT EXISTS (
					SELECT 1 FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_constraint c
					ON (d.refclassid = c.tableoid AND d.refobjid = c.oid)
					WHERE d.classid = pc2.tableoid AND d.objid = pc2.oid AND d.deptype = 'i' AND c.contype IN ('u', 'p')
				) END
				AND pc2.conname ILIKE {$term} {$where}
			UNION ALL
			SELECT 'CONSTRAINTDOMAIN', pt.oid, pn.nspname, pt.typname, pc.conname FROM pg_catalog.pg_type pt, pg_catalog.pg_namespace pn,
				pg_catalog.pg_constraint pc WHERE pt.typnamespace=pn.oid AND pt.oid=pc.contypid AND pc.contypid != 0
				AND pc.conname ILIKE {$term} {$where}
			UNION ALL
			SELECT 'TRIGGER', NULL, pn.nspname, pc.relname, pt.tgname FROM pg_catalog.pg_class pc, pg_catalog.pg_namespace pn,
				pg_catalog.pg_trigger pt WHERE pc.relnamespace=pn.oid AND pc.oid=pt.tgrelid
					AND ( pt.tgconstraint = 0 OR NOT EXISTS
					(SELECT 1 FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_constraint c
					ON (d.refclassid = c.tableoid AND d.refobjid = c.oid)
					WHERE d.classid = pt.tableoid AND d.objid = pt.oid AND d.deptype = 'i' AND c.contype = 'f'))
				AND pt.tgname ILIKE {$term} {$where}
			UNION ALL
			SELECT 'RULETABLE', NULL, pn.nspname AS schemaname, c.relname AS tablename, r.rulename FROM pg_catalog.pg_rewrite r
				JOIN pg_catalog.pg_class c ON c.oid = r.ev_class
				LEFT JOIN pg_catalog.pg_namespace pn ON pn.oid = c.relnamespace
				WHERE c.relkind='r' AND r.rulename != '_RETURN' AND r.rulename ILIKE {$term} {$where}
			UNION ALL
			SELECT 'RULEVIEW', NULL, pn.nspname AS schemaname, c.relname AS tablename, r.rulename FROM pg_catalog.pg_rewrite r
				JOIN pg_catalog.pg_class c ON c.oid = r.ev_class
				LEFT JOIN pg_catalog.pg_namespace pn ON pn.oid = c.relnamespace
				WHERE c.relkind='v' AND r.rulename != '_RETURN' AND r.rulename ILIKE {$term} {$where}
		";

		// Add advanced objects if show_advanced is set
		if ($conf['show_advanced']) {
			$sql .= "
				UNION ALL
				SELECT CASE WHEN pt.typtype='d' THEN 'DOMAIN' ELSE 'TYPE' END, pt.oid, pn.nspname, NULL,
					pt.typname FROM pg_catalog.pg_type pt, pg_catalog.pg_namespace pn
					WHERE pt.typnamespace=pn.oid AND typname ILIKE {$term}
					AND (pt.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = pt.typrelid))
					{$where}
			 	UNION ALL
				SELECT 'OPERATOR', po.oid, pn.nspname, NULL, po.oprname FROM pg_catalog.pg_operator po, pg_catalog.pg_namespace pn
					WHERE po.oprnamespace=pn.oid AND oprname ILIKE {$term} {$where}
				UNION ALL
				SELECT 'CONVERSION', pc.oid, pn.nspname, NULL, pc.conname FROM pg_catalog.pg_conversion pc,
					pg_catalog.pg_namespace pn WHERE pc.connamespace=pn.oid AND conname ILIKE {$term} {$where}
				UNION ALL
				SELECT 'LANGUAGE', pl.oid, NULL, NULL, pl.lanname FROM pg_catalog.pg_language pl
					WHERE lanname ILIKE {$term} {$lan_where}
				UNION ALL
				SELECT DISTINCT ON (p.proname) 'AGGREGATE', p.oid, pn.nspname, NULL, p.proname FROM pg_catalog.pg_proc p
					LEFT JOIN pg_catalog.pg_namespace pn ON p.pronamespace=pn.oid
					WHERE p.proisagg AND p.proname ILIKE {$term} {$where}
				UNION ALL
				SELECT DISTINCT ON (po.opcname) 'OPCLASS', po.oid, pn.nspname, NULL, po.opcname FROM pg_catalog.pg_opclass po,
					pg_catalog.pg_namespace pn WHERE po.opcnamespace=pn.oid
					AND po.opcname ILIKE {$term} {$where}
			";
		}
		// Otherwise just add domains
		else {
			$sql .= "
				UNION ALL
				SELECT 'DOMAIN', pt.oid, pn.nspname, NULL,
					pt.typname FROM pg_catalog.pg_type pt, pg_catalog.pg_namespace pn
					WHERE pt.typnamespace=pn.oid AND pt.typtype='d' AND typname ILIKE {$term}
					AND (pt.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = pt.typrelid))
					{$where}
			";
		}

		if ($filter != '') {
			// We use like to make RULE, CONSTRAINT and COLUMN searches work
			$sql .= ") AS sub WHERE type LIKE '{$filter}%' ";
		}

		$sql .= "ORDER BY type, schemaname, relname, name";

		return $this->selectSet($sql);
	}

	/**
	 * Returns all available variable information.
	 * @return A recordset
	 */
	function getVariables() {
		$sql = "SHOW ALL";

		return $this->selectSet($sql);
	}

	// Schema functons

	/**
	 * Return all schemas in the current database.
	 * @return All schemas, sorted alphabetically
	 */
	function getSchemas() {
		global $conf;

		if (!$conf['show_system']) {
			$where = "WHERE nspname NOT LIKE 'pg@_%' ESCAPE '@' AND nspname != 'information_schema'";

		}
		else $where = "WHERE nspname !~ '^pg_t(emp_[0-9]+|oast)$'";
		$sql = "
			SELECT pn.nspname, pu.rolname AS nspowner,
				pg_catalog.obj_description(pn.oid, 'pg_namespace') AS nspcomment
			FROM pg_catalog.pg_namespace pn
				LEFT JOIN pg_catalog.pg_roles pu ON (pn.nspowner = pu.oid)
			{$where}
			ORDER BY nspname";

		return $this->selectSet($sql);
	}

	/**
	 * Return all information relating to a schema
	 * @param $schema The name of the schema
	 * @return Schema information
	 */
	function getSchemaByName($schema) {
		$this->clean($schema);
		$sql = "
			SELECT nspname, nspowner, r.rolname AS ownername, nspacl,
				pg_catalog.obj_description(pn.oid, 'pg_namespace') as nspcomment
			FROM pg_catalog.pg_namespace pn
				LEFT JOIN pg_roles as r ON pn.nspowner = r.oid
			WHERE nspname='{$schema}'";
		return $this->selectSet($sql);
	}

	/**
	 * Sets the current working schema.  Will also set Class variable.
	 * @param $schema The the name of the schema to work in
	 * @return 0 success
	 */
	function setSchema($schema) {
		// Get the current schema search path, including 'pg_catalog'.
		$search_path = $this->getSearchPath();
		// Prepend $schema to search path
		array_unshift($search_path, $schema);
		$status = $this->setSearchPath($search_path);
		if ($status == 0) {
			$this->_schema = $schema;
			return 0;
		}
		else return $status;
	}

	/**
	 * Sets the current schema search path
	 * @param $paths An array of schemas in required search order
	 * @return 0 success
	 * @return -1 Array not passed
	 * @return -2 Array must contain at least one item
	 */
	function setSearchPath($paths) {
		if (!is_array($paths)) return -1;
		elseif (sizeof($paths) == 0) return -2;
		elseif (sizeof($paths) == 1 && $paths[0] == '') {
			// Need to handle empty paths in some cases
			$paths[0] = 'pg_catalog';
		}

		// Loop over all the paths to check that none are empty
		$temp = array();
		foreach ($paths as $schema) {
			if ($schema != '') $temp[] = $schema;
		}
		$this->fieldArrayClean($temp);

		$sql = 'SET SEARCH_PATH TO "' . implode('","', $temp) . '"';

		return $this->execute($sql);
 		}

	/**
	 * Creates a new schema.
	 * @param $schemaname The name of the schema to create
	 * @param $authorization (optional) The username to create the schema for.
	 * @param $comment (optional) If omitted, defaults to nothing
	 * @return 0 success
	 */
	function createSchema($schemaname, $authorization = '', $comment = '') {
		$this->fieldClean($schemaname);
		$this->fieldClean($authorization);

		$sql = "CREATE SCHEMA \"{$schemaname}\"";
		if ($authorization != '') $sql .= " AUTHORIZATION \"{$authorization}\"";

		if ($comment != '') {
			$status = $this->beginTransaction();
			if ($status != 0) return -1;
		}

		// Create the new schema
		$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		// Set the comment
		if ($comment != '') {
			$status = $this->setComment('SCHEMA', $schemaname, '', $comment);
			if ($status != 0) {
				$this->rollbackTransaction();
				return -1;
			}

		return $this->endTransaction();
		}

		return 0;
	}

	/**
	 * Updates a schema.
	 * @param $schemaname The name of the schema to drop
	 * @param $comment The new comment for this schema
	 * @param $owner The new owner for this schema
	 * @return 0 success
	 */
	function updateSchema($schemaname, $comment, $name, $owner) {
		$this->fieldClean($schemaname);
		$this->fieldClean($name);
		$this->fieldClean($owner);

		$status = $this->beginTransaction();
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		$status = $this->setComment('SCHEMA', $schemaname, '', $comment);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		$schema_rs = $this->getSchemaByName($schemaname);
		/* Only if the owner change */
		if ($schema_rs->fields['ownername'] != $owner) {
			$sql = "ALTER SCHEMA \"{$schemaname}\" OWNER TO \"{$owner}\"";
			$status = $this->execute($sql);
			if ($status != 0) {
				$this->rollbackTransaction();
				return -1;
			}
		}

		// Only if the name has changed
		if ($name != $schemaname) {
			$sql = "ALTER SCHEMA \"{$schemaname}\" RENAME TO \"{$name}\"";
			$status = $this->execute($sql);
			if ($status != 0) {
				$this->rollbackTransaction();
				return -1;
			}
		}

		return $this->endTransaction();
	}

	/**
	 * Drops a schema.
	 * @param $schemaname The name of the schema to drop
	 * @param $cascade True to cascade drop, false to restrict
	 * @return 0 success
	 */
	function dropSchema($schemaname, $cascade) {
		$this->fieldClean($schemaname);

		$sql = "DROP SCHEMA \"{$schemaname}\"";
		if ($cascade) $sql .= " CASCADE";

		return $this->execute($sql);
		}

	/**
	 * Return the current schema search path
	 * @return Array of schema names
	 */
	function getSearchPath() {
		$sql = 'SELECT current_schemas(false) AS search_path';

		return $this->phpArray($this->selectField($sql, 'search_path'));
		}

	// Table functions

    /**
	 * Checks to see whether or not a table has a unique id column
	 * @param $table The table name
	 * @return True if it has a unique id, false otherwise
	 * @return null error
	 **/
	function hasObjectID($table) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);

		$sql = "SELECT relhasoids FROM pg_catalog.pg_class WHERE relname='{$table}'
			AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='{$c_schema}')";

		$rs = $this->selectSet($sql);
		if ($rs->recordCount() != 1) return null;
		else {
			$rs->fields['relhasoids'] = $this->phpBool($rs->fields['relhasoids']);
			return $rs->fields['relhasoids'];
		}
	}

	/**
	 * Returns table information
	 * @param $table The name of the table
	 * @return A recordset
	 */
	function getTable($table) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);

		$sql = "
			SELECT
			  c.relname, n.nspname, u.usename AS relowner,
			  pg_catalog.obj_description(c.oid, 'pg_class') AS relcomment,
			  (SELECT spcname FROM pg_catalog.pg_tablespace pt WHERE pt.oid=c.reltablespace) AS tablespace
			FROM pg_catalog.pg_class c
			     LEFT JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner
			     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind = 'r'
			      AND n.nspname = '{$c_schema}'
			      AND n.oid = c.relnamespace
			      AND c.relname = '{$table}'";

		return $this->selectSet($sql);
	}

	/**
	 * Return all tables in current database (and schema)
	 * @param $all True to fetch all tables, false for just in current schema
	 * @return All tables, sorted alphabetically
	 */
	function getTables($all = false) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		if ($all) {
			// Exclude pg_catalog and information_schema tables
			$sql = "SELECT schemaname AS nspname, tablename AS relname, tableowner AS relowner
					FROM pg_catalog.pg_tables
					WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
					ORDER BY schemaname, tablename";
		} else {
			$sql = "SELECT c.relname, pg_catalog.pg_get_userbyid(c.relowner) AS relowner,
						pg_catalog.obj_description(c.oid, 'pg_class') AS relcomment,
						reltuples::bigint,
						(SELECT spcname FROM pg_catalog.pg_tablespace pt WHERE pt.oid=c.reltablespace) AS tablespace
					FROM pg_catalog.pg_class c
					LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
					WHERE c.relkind = 'r'
					AND nspname='{$c_schema}'
					ORDER BY c.relname";
		}

		return $this->selectSet($sql);
	}

	/**
	 * Retrieve the attribute definition of a table
	 * @param $table The name of the table
	 * @param $field (optional) The name of a field to return
	 * @return All attributes in order
	 */
	function getTableAttributes($table, $field = '') {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);
		$this->clean($field);

		if ($field == '') {
			// This query is made much more complex by the addition of the 'attisserial' field.
			// The subquery to get that field checks to see if there is an internally dependent
			// sequence on the field.
			$sql = "
				SELECT
					a.attname, a.attnum,
					pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
					a.atttypmod,
					a.attnotnull, a.atthasdef, pg_catalog.pg_get_expr(adef.adbin, adef.adrelid, true) as adsrc,
					a.attstattarget, a.attstorage, t.typstorage,
					(
						SELECT 1 FROM pg_catalog.pg_depend pd, pg_catalog.pg_class pc
						WHERE pd.objid=pc.oid
						AND pd.classid=pc.tableoid
						AND pd.refclassid=pc.tableoid
						AND pd.refobjid=a.attrelid
						AND pd.refobjsubid=a.attnum
						AND pd.deptype='i'
						AND pc.relkind='S'
					) IS NOT NULL AS attisserial,
					pg_catalog.col_description(a.attrelid, a.attnum) AS comment
				FROM
					pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef adef
					ON a.attrelid=adef.adrelid
					AND a.attnum=adef.adnum
					LEFT JOIN pg_catalog.pg_type t ON a.atttypid=t.oid
				WHERE
					a.attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='{$table}'
						AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE
						nspname = '{$c_schema}'))
					AND a.attnum > 0 AND NOT a.attisdropped
				ORDER BY a.attnum";
		}
		else {
			$sql = "
				SELECT
					a.attname, a.attnum,
					pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
					pg_catalog.format_type(a.atttypid, NULL) as base_type,
					a.atttypmod,
					a.attnotnull, a.atthasdef, pg_catalog.pg_get_expr(adef.adbin, adef.adrelid, true) as adsrc,
					a.attstattarget, a.attstorage, t.typstorage,
					pg_catalog.col_description(a.attrelid, a.attnum) AS comment
				FROM
					pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef adef
					ON a.attrelid=adef.adrelid
					AND a.attnum=adef.adnum
					LEFT JOIN pg_catalog.pg_type t ON a.atttypid=t.oid
				WHERE
					a.attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='{$table}'
						AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE
						nspname = '{$c_schema}'))
					AND a.attname = '{$field}'";
		}

		return $this->selectSet($sql);
	}

	/**
	 * Finds the names and schemas of parent tables (in order)
	 * @param $table The table to find the parents for
	 * @return A recordset
	 */
	function getTableParents($table) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);

		$sql = "
			SELECT
				pn.nspname, relname
			FROM
				pg_catalog.pg_class pc, pg_catalog.pg_inherits pi, pg_catalog.pg_namespace pn
			WHERE
				pc.oid=pi.inhparent
				AND pc.relnamespace=pn.oid
				AND pi.inhrelid = (SELECT oid from pg_catalog.pg_class WHERE relname='{$table}'
					AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '{$c_schema}'))
			ORDER BY
				pi.inhseqno
		";

		return $this->selectSet($sql);
	}

	/**
	 * Finds the names and schemas of child tables
	 * @param $table The table to find the children for
	 * @return A recordset
	 */
	function getTableChildren($table) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);

		$sql = "
			SELECT
				pn.nspname, relname
			FROM
				pg_catalog.pg_class pc, pg_catalog.pg_inherits pi, pg_catalog.pg_namespace pn
			WHERE
				pc.oid=pi.inhrelid
				AND pc.relnamespace=pn.oid
				AND pi.inhparent = (SELECT oid from pg_catalog.pg_class WHERE relname='{$table}'
					AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '{$c_schema}'))
		";

		return $this->selectSet($sql);
	}

	/**
	 * Returns the SQL definition for the table.
	 * @pre MUST be run within a transaction
	 * @param $table The table to define
	 * @param $clean True to issue drop command, false otherwise
	 * @return A string containing the formatted SQL code
	 * @return null On error
	 */
	function getTableDefPrefix($table, $clean = false) {
		// Fetch table
		$t = $this->getTable($table);
		if (!is_object($t) || $t->recordCount() != 1) {
			$this->rollbackTransaction();
			return null;
		}
		$this->fieldClean($t->fields['relname']);
		$this->fieldClean($t->fields['nspname']);

		// Fetch attributes
		$atts = $this->getTableAttributes($table);
		if (!is_object($atts)) {
			$this->rollbackTransaction();
			return null;
		}

		// Fetch constraints
		$cons = $this->getConstraints($table);
		if (!is_object($cons)) {
			$this->rollbackTransaction();
			return null;
		}

		// Output a reconnect command to create the table as the correct user
		$sql = $this->getChangeUserSQL($t->fields['relowner']) . "\n\n";

		// Set schema search path
		$sql .= "SET search_path = \"{$t->fields['nspname']}\", pg_catalog;\n\n";

		// Begin CREATE TABLE definition
		$sql .= "-- Definition\n\n";
		// DROP TABLE must be fully qualified in case a table with the same name exists
		// in pg_catalog.
		if (!$clean) $sql .= "-- ";
		$sql .= "DROP TABLE ";
		$sql .= "\"{$t->fields['nspname']}\".\"{$t->fields['relname']}\";\n";
		$sql .= "CREATE TABLE \"{$t->fields['nspname']}\".\"{$t->fields['relname']}\" (\n";

		// Output all table columns
		$col_comments_sql = '';   // Accumulate comments on columns
		$num = $atts->recordCount() + $cons->recordCount();
		$i = 1;
		while (!$atts->EOF) {
			$this->fieldClean($atts->fields['attname']);
			$sql .= "    \"{$atts->fields['attname']}\"";
			// Dump SERIAL and BIGSERIAL columns correctly
			if ($this->phpBool($atts->fields['attisserial']) &&
					($atts->fields['type'] == 'integer' || $atts->fields['type'] == 'bigint')) {
				if ($atts->fields['type'] == 'integer')
					$sql .= " SERIAL";
				else
					$sql .= " BIGSERIAL";
			}
			else {
				$sql .= " " . $this->formatType($atts->fields['type'], $atts->fields['atttypmod']);

				// Add NOT NULL if necessary
				if ($this->phpBool($atts->fields['attnotnull']))
					$sql .= " NOT NULL";
				// Add default if necessary
				if ($atts->fields['adsrc'] !== null)
					$sql .= " DEFAULT {$atts->fields['adsrc']}";
			}

			// Output comma or not
			if ($i < $num) $sql .= ",\n";
			else $sql .= "\n";

			// Does this column have a comment?
			if ($atts->fields['comment'] !== null) {
				$this->clean($atts->fields['comment']);
				$col_comments_sql .= "COMMENT ON COLUMN \"{$t->fields['relname']}\".\"{$atts->fields['attname']}\"  IS '{$atts->fields['comment']}';\n";
			}

			$atts->moveNext();
			$i++;
		}
		// Output all table constraints
		while (!$cons->EOF) {
			$this->fieldClean($cons->fields['conname']);
			$sql .= "    CONSTRAINT \"{$cons->fields['conname']}\" ";
			// Nasty hack to support pre-7.4 PostgreSQL
			if ($cons->fields['consrc'] !== null)
				$sql .= $cons->fields['consrc'];
			else {
				switch ($cons->fields['contype']) {
					case 'p':
						$keys = $this->getAttributeNames($table, explode(' ', $cons->fields['indkey']));
						$sql .= "PRIMARY KEY (" . join(',', $keys) . ")";
						break;
					case 'u':
						$keys = $this->getAttributeNames($table, explode(' ', $cons->fields['indkey']));
						$sql .= "UNIQUE (" . join(',', $keys) . ")";
						break;
					default:
						// Unrecognised constraint
						$this->rollbackTransaction();
						return null;
				}
			}

			// Output comma or not
			if ($i < $num) $sql .= ",\n";
			else $sql .= "\n";

			$cons->moveNext();
			$i++;
		}

		$sql .= ")";

		// @@@@ DUMP CLUSTERING INFORMATION

		// Inherits
		/*
		 * XXX: This is currently commented out as handling inheritance isn't this simple.
		 * You also need to make sure you don't dump inherited columns and defaults, as well
		 * as inherited NOT NULL and CHECK constraints.  So for the time being, we just do
		 * not claim to support inheritance.
		$parents = $this->getTableParents($table);
		if ($parents->recordCount() > 0) {
			$sql .= " INHERITS (";
			while (!$parents->EOF) {
				$this->fieldClean($parents->fields['relname']);
				// Qualify the parent table if it's in another schema
				if ($parents->fields['schemaname'] != $this->_schema) {
					$this->fieldClean($parents->fields['schemaname']);
					$sql .= "\"{$parents->fields['schemaname']}\".";
				}
				$sql .= "\"{$parents->fields['relname']}\"";

				$parents->moveNext();
				if (!$parents->EOF) $sql .= ', ';
			}
			$sql .= ")";
		}
		*/

		// Handle WITHOUT OIDS
		if ($this->hasObjectID($table))
			$sql .= " WITH OIDS";
		else
			$sql .= " WITHOUT OIDS";

		$sql .= ";\n";

		// Column storage and statistics
		$atts->moveFirst();
		$first = true;
		while (!$atts->EOF) {
			$this->fieldClean($atts->fields['attname']);
			// Statistics first
			if ($atts->fields['attstattarget'] >= 0) {
				if ($first) {
					$sql .= "\n";
					$first = false;
				}
				$sql .= "ALTER TABLE ONLY \"{$t->fields['nspname']}\".\"{$t->fields['relname']}\" ALTER COLUMN \"{$atts->fields['attname']}\" SET STATISTICS {$atts->fields['attstattarget']};\n";
			}
			// Then storage
			if ($atts->fields['attstorage'] != $atts->fields['typstorage']) {
				switch ($atts->fields['attstorage']) {
					case 'p':
						$storage = 'PLAIN';
						break;
					case 'e':
						$storage = 'EXTERNAL';
						break;
					case 'm':
						$storage = 'MAIN';
						break;
					case 'x':
						$storage = 'EXTENDED';
						break;
					default:
						// Unknown storage type
						$this->rollbackTransaction();
						return null;
				}
				$sql .= "ALTER TABLE ONLY \"{$t->fields['nspname']}\".\"{$t->fields['relname']}\" ALTER COLUMN \"{$atts->fields['attname']}\" SET STORAGE {$storage};\n";
			}

			$atts->moveNext();
		}

		// Comment
		if ($t->fields['relcomment'] !== null) {
			$this->clean($t->fields['relcomment']);
			$sql .= "\n-- Comment\n\n";
			$sql .= "COMMENT ON TABLE \"{$t->fields['nspname']}\".\"{$t->fields['relname']}\" IS '{$t->fields['relcomment']}';\n";
		}

		// Add comments on columns, if any
		if ($col_comments_sql != '') $sql .= $col_comments_sql;

		// Privileges
		$privs = $this->getPrivileges($table, 'table');
		if (!is_array($privs)) {
			$this->rollbackTransaction();
			return null;
		}

		if (sizeof($privs) > 0) {
			$sql .= "\n-- Privileges\n\n";
			/*
			 * Always start with REVOKE ALL FROM PUBLIC, so that we don't have to
			 * wire-in knowledge about the default public privileges for different
			 * kinds of objects.
			 */
			$sql .= "REVOKE ALL ON TABLE \"{$t->fields['nspname']}\".\"{$t->fields['relname']}\" FROM PUBLIC;\n";
			foreach ($privs as $v) {
				// Get non-GRANT OPTION privs
				$nongrant = array_diff($v[2], $v[4]);

				// Skip empty or owner ACEs
				if (sizeof($v[2]) == 0 || ($v[0] == 'user' && $v[1] == $t->fields['relowner'])) continue;

				// Change user if necessary
				if ($this->hasGrantOption() && $v[3] != $t->fields['relowner']) {
					$grantor = $v[3];
					$this->clean($grantor);
					$sql .= "SET SESSION AUTHORIZATION '{$grantor}';\n";
				}

				// Output privileges with no GRANT OPTION
				$sql .= "GRANT " . join(', ', $nongrant) . " ON TABLE \"{$t->fields['relname']}\" TO ";
				switch ($v[0]) {
					case 'public':
						$sql .= "PUBLIC;\n";
						break;
					case 'user':
						$this->fieldClean($v[1]);
						$sql .= "\"{$v[1]}\";\n";
						break;
					case 'group':
						$this->fieldClean($v[1]);
						$sql .= "GROUP \"{$v[1]}\";\n";
						break;
					default:
						// Unknown privilege type - fail
						$this->rollbackTransaction();
						return null;
				}

				// Reset user if necessary
				if ($this->hasGrantOption() && $v[3] != $t->fields['relowner']) {
					$sql .= "RESET SESSION AUTHORIZATION;\n";
				}

				// Output privileges with GRANT OPTION

				// Skip empty or owner ACEs
				if (!$this->hasGrantOption() || sizeof($v[4]) == 0) continue;

				// Change user if necessary
				if ($this->hasGrantOption() && $v[3] != $t->fields['relowner']) {
					$grantor = $v[3];
					$this->clean($grantor);
					$sql .= "SET SESSION AUTHORIZATION '{$grantor}';\n";
				}

				$sql .= "GRANT " . join(', ', $v[4]) . " ON \"{$t->fields['relname']}\" TO ";
				switch ($v[0]) {
					case 'public':
						$sql .= "PUBLIC";
						break;
					case 'user':
						$this->fieldClean($v[1]);
						$sql .= "\"{$v[1]}\"";
						break;
					case 'group':
						$this->fieldClean($v[1]);
						$sql .= "GROUP \"{$v[1]}\"";
						break;
					default:
						// Unknown privilege type - fail
						return null;
				}
				$sql .= " WITH GRANT OPTION;\n";

				// Reset user if necessary
				if ($this->hasGrantOption() && $v[3] != $t->fields['relowner']) {
					$sql .= "RESET SESSION AUTHORIZATION;\n";
				}

			}
		}

		// Add a newline to separate data that follows (if any)
		$sql .= "\n";

		return $sql;
	}

	/**
	 * Returns extra table definition information that is most usefully
	 * dumped after the table contents for speed and efficiency reasons
	 * @param $table The table to define
	 * @return A string containing the formatted SQL code
	 * @return null On error
	 */
	function getTableDefSuffix($table) {
		$sql = '';

		// Indexes
		$indexes = $this->getIndexes($table);
		if (!is_object($indexes)) {
			$this->rollbackTransaction();
			return null;
		}

		if ($indexes->recordCount() > 0) {
			$sql .= "\n-- Indexes\n\n";
			while (!$indexes->EOF) {
				$sql .= $indexes->fields['inddef'] . ";\n";

				$indexes->moveNext();
			}
		}

		// Triggers
		$triggers = $this->getTriggers($table);
		if (!is_object($triggers)) {
			$this->rollbackTransaction();
			return null;
		}

		if ($triggers->recordCount() > 0) {
			$sql .= "\n-- Triggers\n\n";
			while (!$triggers->EOF) {

				$sql .= $triggers->fields['tgdef'];
				$sql .= ";\n";

				$triggers->moveNext();
			}
		}

		// Rules
		$rules = $this->getRules($table);
		if (!is_object($rules)) {
			$this->rollbackTransaction();
			return null;
		}

		if ($rules->recordCount() > 0) {
			$sql .= "\n-- Rules\n\n";
			while (!$rules->EOF) {
				$sql .= $rules->fields['definition'] . "\n";

				$rules->moveNext();
			}
		}

		return $sql;
	}

	/**
	 * Creates a new table in the database
	 * @param $name The name of the table
	 * @param $fields The number of fields
	 * @param $field An array of field names
	 * @param $type An array of field types
	 * @param $array An array of '' or '[]' for each type if it's an array or not
	 * @param $length An array of field lengths
	 * @param $notnull An array of not null
	 * @param $default An array of default values
	 * @param $withoutoids True if WITHOUT OIDS, false otherwise
	 * @param $colcomment An array of comments
	 * @param $comment Table comment
	 * @param $tablespace The tablespace name ('' means none/default)
 	 * @param $uniquekey An Array indicating the fields that are unique (those indexes that are set)
 	 * @param $primarykey An Array indicating the field used for the primarykey (those indexes that are set)
	 * @return 0 success
	 * @return -1 no fields supplied
	 */
	function createTable($name, $fields, $field, $type, $array, $length, $notnull,
				$default, $withoutoids, $colcomment, $tblcomment, $tablespace,
				$uniquekey, $primarykey) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($name);

		$status = $this->beginTransaction();
		if ($status != 0) return -1;

		$found = false;
		$first = true;
		$comment_sql = ''; //Accumulate comments for the columns
		$sql = "CREATE TABLE \"{$f_schema}\".\"{$name}\" (";
		for ($i = 0; $i < $fields; $i++) {
			$this->fieldClean($field[$i]);
			$this->clean($type[$i]);
			$this->clean($length[$i]);
			$this->clean($colcomment[$i]);

			// Skip blank columns - for user convenience
			if ($field[$i] == '' || $type[$i] == '') continue;
			// If not the first column, add a comma
			if (!$first) $sql .= ", ";
			else $first = false;

			switch ($type[$i]) {
				// Have to account for weird placing of length for with/without
				// time zone types
				case 'timestamp with time zone':
				case 'timestamp without time zone':
					$qual = substr($type[$i], 9);
					$sql .= "\"{$field[$i]}\" timestamp";
					if ($length[$i] != '') $sql .= "({$length[$i]})";
					$sql .= $qual;
					break;
				case 'time with time zone':
				case 'time without time zone':
					$qual = substr($type[$i], 4);
					$sql .= "\"{$field[$i]}\" time";
					if ($length[$i] != '') $sql .= "({$length[$i]})";
					$sql .= $qual;
					break;
				default:
					$sql .= "\"{$field[$i]}\" {$type[$i]}";
					if ($length[$i] != '') $sql .= "({$length[$i]})";
			}
			// Add array qualifier if necessary
			if ($array[$i] == '[]') $sql .= '[]';
			// Add other qualifiers
			if (!isset($primarykey[$i])) {
 				if (isset($uniquekey[$i])) $sql .= " UNIQUE";
 				if (isset($notnull[$i])) $sql .= " NOT NULL";
			}
			if ($default[$i] != '') $sql .= " DEFAULT {$default[$i]}";

			if ($colcomment[$i] != '') $comment_sql .= "COMMENT ON COLUMN \"{$name}\".\"{$field[$i]}\" IS '{$colcomment[$i]}';\n";

			$found = true;
		}

		if (!$found) return -1;

		// PRIMARY KEY
 		$primarykeycolumns = array();
 		for ($i = 0; $i < $fields; $i++) {
 			if (isset($primarykey[$i])) {
 				$primarykeycolumns[] = "\"{$field[$i]}\"";
			}
		}
 		if (count($primarykeycolumns) > 0) {
 			$sql .= ", PRIMARY KEY (" . implode(", ", $primarykeycolumns) . ")";
		}

		$sql .= ")";

		// WITHOUT OIDS
		if ($withoutoids)
			$sql .= ' WITHOUT OIDS';
		else
			$sql .= ' WITH OIDS';

		// Tablespace
		if ($this->hasTablespaces() && $tablespace != '') {
			$this->fieldClean($tablespace);
			$sql .= " TABLESPACE \"{$tablespace}\"";
		}

		$status = $this->execute($sql);
		if ($status) {
			$this->rollbackTransaction();
			return -1;
		}

		if ($tblcomment != '') {
			$status = $this->setComment('TABLE', '', $name, $tblcomment, true);
			if ($status) {
				$this->rollbackTransaction();
				return -1;
			}
		}

		if ($comment_sql != '') {
			$status = $this->execute($comment_sql);
			if ($status) {
				$this->rollbackTransaction();
				return -1;
			}
		}
		return $this->endTransaction();
	}

	/**
	 * Creates a new table in the database copying attribs and other properties from another table
	 * @param $name The name of the table
	 * @param $like an array giving the schema ans the name of the table from which attribs are copying from:
	 *		array(
	 *			'table' => table name,
	 *			'schema' => the schema name,
	 *		)
	 * @param $defaults if true, copy the defaults values as well
	 * @param $constraints if true, copy the constraints as well (CHECK on table & attr)
	 * @param $tablespace The tablespace name ('' means none/default)
	 */
	function createTableLike($name, $like, $defaults = false, $constraints = false, $idx = false, $tablespace = '') {

		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($name);
		$this->fieldClean($like['schema']);
		$this->fieldClean($like['table']);
		$like = "\"{$like['schema']}\".\"{$like['table']}\"";

		$status = $this->beginTransaction();
		if ($status != 0) return -1;

		$sql = "CREATE TABLE \"{$f_schema}\".\"{$name}\" (LIKE {$like}";

		if ($defaults) $sql .= " INCLUDING DEFAULTS";
		if ($this->hasCreateTableLikeWithConstraints() && $constraints) $sql .= " INCLUDING CONSTRAINTS";
		if ($this->hasCreateTableLikeWithIndexes() && $idx) $sql .= " INCLUDING INDEXES";

		$sql .= ")";

		if ($this->hasTablespaces() && $tablespace != '') {
			$this->fieldClean($tablespace);
			$sql .= " TABLESPACE \"{$tablespace}\"";
		}

		$status = $this->execute($sql);
		if ($status) {
			$this->rollbackTransaction();
			return -1;
		}

		return $this->endTransaction();
	}

	/**
	 * Alter a table's name
	 * /!\ this function is called from _alterTable which take care of escaping fields
	 * @param $tblrs The table RecordSet returned by getTable()
	 * @param $name The new table's name
	 * @return 0 success
	 */
	function alterTableName($tblrs, $name = null) {
		/* vars cleaned in _alterTable */
		// Rename (only if name has changed)
		if (!empty($name) && ($name != $tblrs->fields['relname'])) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			
			$sql = "ALTER TABLE \"{$f_schema}\".\"{$tblrs->fields['relname']}\" RENAME TO \"{$name}\"";
			$status =  $this->execute($sql);
			if ($status == 0)
				$tblrs->fields['relname'] = $name;
			else
				return $status;
		}
		return 0;
	}

	/**
	 * Alter a table's owner
	 * /!\ this function is called from _alterTable which take care of escaping fields
	 * @param $tblrs The table RecordSet returned by getTable()
	 * @param $name The new table's owner
	 * @return 0 success
	 */
	function alterTableOwner($tblrs, $owner = null) {
		/* vars cleaned in _alterTable */
		if (!empty($owner) && ($tblrs->fields['relowner'] != $owner)) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			// If owner has been changed, then do the alteration.  We are
			// careful to avoid this generally as changing owner is a
			// superuser only function.
			$sql = "ALTER TABLE \"{$f_schema}\".\"{$tblrs->fields['relname']}\" OWNER TO \"{$owner}\"";

			return $this->execute($sql);
		}
		return 0;
	}

	/**
	 * Alter a table's tablespace
	 * /!\ this function is called from _alterTable which take care of escaping fields
	 * @param $tblrs The table RecordSet returned by getTable()
	 * @param $name The new table's tablespace
	 * @return 0 success
	 */
	function alterTableTablespace($tblrs, $tablespace = null) {
		/* vars cleaned in _alterTable */
		if (!empty($tablespace) && ($tblrs->fields['tablespace'] != $tablespace)) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			
			// If tablespace has been changed, then do the alteration.  We
			// don't want to do this unnecessarily.
			$sql = "ALTER TABLE \"{$f_schema}\".\"{$tblrs->fields['relname']}\" SET TABLESPACE \"{$tablespace}\"";

			return $this->execute($sql);
		}
		return 0;
	}

	/**
	 * Alter a table's schema
	 * /!\ this function is called from _alterTable which take care of escaping fields
	 * @param $tblrs The table RecordSet returned by getTable()
	 * @param $name The new table's schema
	 * @return 0 success
	 */
	function alterTableSchema($tblrs, $schema = null) {
		/* vars cleaned in _alterTable */
		if (!empty($schema) && ($tblrs->fields['nspname'] != $schema)) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			// If tablespace has been changed, then do the alteration.  We
			// don't want to do this unnecessarily.
			$sql = "ALTER TABLE \"{$f_schema}\".\"{$tblrs->fields['relname']}\" SET SCHEMA \"{$schema}\"";

			return $this->execute($sql);
			}
		return 0;
		}

	/**
	 * Protected method which alter a table
	 * SHOULDN'T BE CALLED OUTSIDE OF A TRANSACTION
	 * @param $tblrs The table recordSet returned by getTable()
	 * @param $name The new name for the table
	 * @param $owner The new owner for the table
	 * @param $schema The new schema for the table
	 * @param $comment The comment on the table
	 * @param $tablespace The new tablespace for the table ('' means leave as is)
	 * @return 0 success
	 * @return -3 rename error
	 * @return -4 comment error
	 * @return -5 owner error
	 * @return -6 tablespace error
	 * @return -7 schema error
	 */
	protected
	function _alterTable($tblrs, $name, $owner, $schema, $comment, $tablespace) {

		$this->fieldArrayClean($tblrs->fields);

		// Comment
		$status = $this->setComment('TABLE', '', $tblrs->fields['relname'], $comment);
		if ($status != 0) return -4;

		// Owner
		$this->fieldClean($owner);
		$status = $this->alterTableOwner($tblrs, $owner);
		if ($status != 0) return -5;

		// Tablespace
		$this->fieldClean($tablespace);
		$status = $this->alterTableTablespace($tblrs, $tablespace);
		if ($status != 0) return -6;

		// Rename
		$this->fieldClean($name);
		$status = $this->alterTableName($tblrs, $name);
		if ($status != 0) return -3;

		// Schema
		$this->fieldClean($schema);
		$status = $this->alterTableSchema($tblrs, $schema);
		if ($status != 0) return -7;

		return 0;
	}

	/**
	 * Alter table properties
	 * @param $table The name of the table
	 * @param $name The new name for the table
	 * @param $owner The new owner for the table
	 * @param $schema The new schema for the table
	 * @param $comment The comment on the table
	 * @param $tablespace The new tablespace for the table ('' means leave as is)
	 * @return 0 success
	 * @return -1 transaction error
	 * @return -2 get existing table error
	 * @return $this->_alterTable error code
	 */
	function alterTable($table, $name, $owner, $schema, $comment, $tablespace) {

		$data = $this->getTable($table);

		if ($data->recordCount() != 1)
			return -2;

		$status = $this->beginTransaction();
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		$status = $this->_alterTable($data, $name, $owner, $schema, $comment, $tablespace);

		if ($status != 0) {
			$this->rollbackTransaction();
			return $status;
		}

		return $this->endTransaction();
	}

	/**
	 * Returns the SQL for changing the current user
	 * @param $user The user to change to
	 * @return The SQL
	 */
	function getChangeUserSQL($user) {
		$this->clean($user);
		return "SET SESSION AUTHORIZATION '{$user}';";
	}

	/**
	 * Given an array of attnums and a relation, returns an array mapping
	 * attribute number to attribute name.
	 * @param $table The table to get attributes for
	 * @param $atts An array of attribute numbers
	 * @return An array mapping attnum to attname
	 * @return -1 $atts must be an array
	 * @return -2 wrong number of attributes found
	 */
	function getAttributeNames($table, $atts) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);
		$this->arrayClean($atts);

		if (!is_array($atts)) return -1;

		if (sizeof($atts) == 0) return array();

		$sql = "SELECT attnum, attname FROM pg_catalog.pg_attribute WHERE
			attrelid=(SELECT oid FROM pg_catalog.pg_class WHERE relname='{$table}' AND
			relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='{$c_schema}'))
			AND attnum IN ('" . join("','", $atts) . "')";

		$rs = $this->selectSet($sql);
		if ($rs->recordCount() != sizeof($atts)) {
				return -2;
			}
		else {
			$temp = array();
			while (!$rs->EOF) {
				$temp[$rs->fields['attnum']] = $rs->fields['attname'];
				$rs->moveNext();
			}
			return $temp;
		}
	}

	/**
	 * Empties a table in the database
	 * @param $table The table to be emptied
	 * @return 0 success
	 */
	function emptyTable($table) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);

		$sql = "DELETE FROM \"{$f_schema}\".\"{$table}\"";

		return $this->execute($sql);
	}

	/**
	 * Removes a table from the database
	 * @param $table The table to drop
	 * @param $cascade True to cascade drop, false to restrict
	 * @return 0 success
	 */
	function dropTable($table, $cascade) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);

		$sql = "DROP TABLE \"{$f_schema}\".\"{$table}\"";
		if ($cascade) $sql .= " CASCADE";

			return $this->execute($sql);
		}

	/**
	 * Add a new column to a table
	 * @param $table The table to add to
	 * @param $column The name of the new column
	 * @param $type The type of the column
	 * @param $array True if array type, false otherwise
	 * @param $notnull True if NOT NULL, false otherwise
	 * @param $default The default for the column.  '' for none.
	 * @param $length The optional size of the column (ie. 30 for varchar(30))
	 * @return 0 success
	 */
	function addColumn($table, $column, $type, $array, $length, $notnull, $default, $comment) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldClean($column);
		$this->clean($type);
		$this->clean($length);

		if ($length == '')
			$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ADD COLUMN \"{$column}\" {$type}";
		else {
			switch ($type) {
				// Have to account for weird placing of length for with/without
				// time zone types
				case 'timestamp with time zone':
				case 'timestamp without time zone':
					$qual = substr($type, 9);
					$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ADD COLUMN \"{$column}\" timestamp({$length}){$qual}";
					break;
				case 'time with time zone':
				case 'time without time zone':
					$qual = substr($type, 4);
					$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ADD COLUMN \"{$column}\" time({$length}){$qual}";
					break;
				default:
					$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ADD COLUMN \"{$column}\" {$type}({$length})";
			}
		}

		// Add array qualifier, if requested
		if ($array) $sql .= '[]';

		// If we have advanced column adding, add the extra qualifiers
		if ($this->hasCreateFieldWithConstraints()) {
			// NOT NULL clause
			if ($notnull) $sql .= ' NOT NULL';

			// DEFAULT clause
			if ($default != '') $sql .= ' DEFAULT ' . $default;
		}

		$status = $this->beginTransaction();
		if ($status != 0) return -1;

		$status = $this->execute($sql);
		if ($status != 0) {
				$this->rollbackTransaction();
				return -1;
			}

		$status = $this->setComment('COLUMN', $column, $table, $comment);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
	}

		return $this->endTransaction();
	}

	/**
	 * Alters a column in a table
	 * @param $table The table in which the column resides
	 * @param $column The column to alter
	 * @param $name The new name for the column
	 * @param $notnull (boolean) True if not null, false otherwise
	 * @param $oldnotnull (boolean) True if column is already not null, false otherwise
	 * @param $default The new default for the column
	 * @param $olddefault The old default for the column
	 * @param $type The new type for the column
	 * @param $array True if array type, false otherwise
	 * @param $length The optional size of the column (ie. 30 for varchar(30))
	 * @param $oldtype The old type for the column
	 * @param $comment Comment for the column
	 * @return 0 success
	 * @return -1 batch alteration failed
	 * @return -4 rename column error
	 * @return -5 comment error
	 * @return -6 transaction error
	 */
	function alterColumn($table, $column, $name, $notnull, $oldnotnull, $default, $olddefault,
		$type, $length, $array, $oldtype, $comment)
	{
		// Begin transaction
		$status = $this->beginTransaction();
		if ($status != 0) {
			$this->rollbackTransaction();
			return -6;
		}

		// Rename the column, if it has been changed
		if ($column != $name) {
			$status = $this->renameColumn($table, $column, $name);
			if ($status != 0) {
				$this->rollbackTransaction();
				return -4;
			}
		}

		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($name);
		$this->fieldClean($table);
		$this->fieldClean($column);

		$toAlter = array();
		// Create the command for changing nullability
		if ($notnull != $oldnotnull) {
			$toAlter[] = "ALTER COLUMN \"{$name}\" ". (($notnull) ? 'SET' : 'DROP') . " NOT NULL";
		}

		// Add default, if it has changed
		if ($default != $olddefault) {
			if ($default == '') {
				$toAlter[] = "ALTER COLUMN \"{$name}\" DROP DEFAULT";
			}
			else {
				$toAlter[] = "ALTER COLUMN \"{$name}\" SET DEFAULT {$default}";
			}
		}

		// Add type, if it has changed
		if ($length == '')
			$ftype = $type;
		else {
			switch ($type) {
				// Have to account for weird placing of length for with/without
				// time zone types
				case 'timestamp with time zone':
				case 'timestamp without time zone':
					$qual = substr($type, 9);
					$ftype = "timestamp({$length}){$qual}";
					break;
				case 'time with time zone':
				case 'time without time zone':
					$qual = substr($type, 4);
					$ftype = "time({$length}){$qual}";
					break;
				default:
					$ftype = "{$type}({$length})";
			}
		}

		// Add array qualifier, if requested
		if ($array) $ftype .= '[]';

		if ($ftype != $oldtype) {
			$toAlter[] = "ALTER COLUMN \"{$name}\" TYPE {$ftype}";
		}

		// Attempt to process the batch alteration, if anything has been changed
		if (!empty($toAlter)) {
			// Initialise an empty SQL string
			$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" "
				. implode(',', $toAlter);
	
			$status = $this->execute($sql);
			if ($status != 0) {
				$this->rollbackTransaction();
				return -1;
			}
		}

		// Update the comment on the column
		$status = $this->setComment('COLUMN', $name, $table, $comment);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -5;
		}

		return $this->endTransaction();
	}

	/**
	 * Renames a column in a table
	 * @param $table The table containing the column to be renamed
	 * @param $column The column to be renamed
	 * @param $newName The new name for the column
	 * @return 0 success
	 */
	function renameColumn($table, $column, $newName) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldClean($column);
		$this->fieldClean($newName);

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" RENAME COLUMN \"{$column}\" TO \"{$newName}\"";

		return $this->execute($sql);
	}

	/**
	 * Sets default value of a column
	 * @param $table The table from which to drop
	 * @param $column The column name to set
	 * @param $default The new default value
	 * @return 0 success
	 */
	function setColumnDefault($table, $column, $default) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldClean($column);

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ALTER COLUMN \"{$column}\" SET DEFAULT {$default}";

		return $this->execute($sql);
	}

	/**
	 * Sets whether or not a column can contain NULLs
	 * @param $table The table that contains the column
	 * @param $column The column to alter
	 * @param $state True to set null, false to set not null
	 * @return 0 success
	 */
	function setColumnNull($table, $column, $state) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldClean($column);

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ALTER COLUMN \"{$column}\" " . (($state) ? 'DROP' : 'SET') . " NOT NULL";

		return $this->execute($sql);
	}

	/**
	 * Drops a column from a table
	 * @param $table The table from which to drop a column
	 * @param $column The column to be dropped
	 * @param $cascade True to cascade drop, false to restrict
	 * @return 0 success
	 */
	function dropColumn($table, $column, $cascade) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldClean($column);

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" DROP COLUMN \"{$column}\"";
		if ($cascade) $sql .= " CASCADE";

		return $this->execute($sql);
	}

	/**
	 * Drops default value of a column
	 * @param $table The table from which to drop
	 * @param $column The column name to drop default
	 * @return 0 success
	 */
	function dropColumnDefault($table, $column) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldClean($column);

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ALTER COLUMN \"{$column}\" DROP DEFAULT";

		return $this->execute($sql);
	}

	/**
	 * Sets up the data object for a dump.  eg. Starts the appropriate
	 * transaction, sets variables, etc.
	 * @return 0 success
	 */
	function beginDump() {
		// Begin serializable transaction (to dump consistent data)
		$status = $this->beginTransaction();
		if ($status != 0) return -1;

		// Set serializable
		$sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE";
		$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		// Set datestyle to ISO
		$sql = "SET DATESTYLE = ISO";
		$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}
		
		// Set extra_float_digits to 2
		$sql = "SET extra_float_digits TO 2";
		$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}
		
		return 0;
	}

	/**
	 * Ends the data object for a dump.
	 * @return 0 success
	 */
	function endDump() {
		return $this->endTransaction();
	}

	/**
	 * Returns a recordset of all columns in a relation.  Used for data export.
	 * @@ Note: Really needs to use a cursor
	 * @param $relation The name of a relation
	 * @return A recordset on success
	 * @return -1 Failed to set datestyle
	 */
	function dumpRelation($relation, $oids) {
		$this->fieldClean($relation);

		// Actually retrieve the rows
		if ($oids) $oid_str = $this->id . ', ';
		else $oid_str = '';

		return $this->selectSet("SELECT {$oid_str}* FROM \"{$relation}\"");
	}
	
	/**
	 * Returns all available autovacuum per table information.
	 * @param $table if given, return autovacuum info for the given table or return all informations for all table
	 *   
	 * @return A recordset
	 */
	function getTableAutovacuum($table='') {

		$sql = '';

		if ($table !== '') {
			$this->clean($table);
			$c_schema = $this->_schema;
			$this->clean($c_schema);

			$sql = "SELECT c.oid, nspname, relname, pg_catalog.array_to_string(reloptions, E',') AS reloptions
				FROM pg_class c
					LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relkind = 'r'::\"char\"
					AND n.nspname NOT IN ('pg_catalog','information_schema')
					AND c.reloptions IS NOT NULL
					AND c.relname = '{$table}' AND n.nspname = '{$c_schema}'
				ORDER BY nspname, relname";
		}
		else {
			$sql = "SELECT c.oid, nspname, relname, pg_catalog.array_to_string(reloptions, E',') AS reloptions
				FROM pg_class c
					LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relkind = 'r'::\"char\"
					AND n.nspname NOT IN ('pg_catalog','information_schema')
					AND c.reloptions IS NOT NULL
				ORDER BY nspname, relname";

		}

		/* tmp var to parse the results */
		$_autovacs = $this->selectSet($sql);

		/* result aray to return as RS */
		$autovacs = array();
		while (!$_autovacs->EOF) {
			$_ = array(
				'nspname' => $_autovacs->fields['nspname'],
				'relname' => $_autovacs->fields['relname']
			);

			foreach (explode(',', $_autovacs->fields['reloptions']) as $var) {
				list($o, $v) = explode('=', $var);
				$_[$o] = $v; 
			}

			$autovacs[] = $_;
			
			$_autovacs->moveNext();
		}

		include_once('./classes/ArrayRecordSet.php');
		return new ArrayRecordSet($autovacs);
	}

	// Row functions

	/**
	 * Get the fields for uniquely identifying a row in a table
	 * @param $table The table for which to retrieve the identifier
	 * @return An array mapping attribute number to attribute name, empty for no identifiers
	 * @return -1 error
	 */
	function getRowIdentifier($table) {
		$oldtable = $table;
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);

		$status = $this->beginTransaction();
		if ($status != 0) return -1;

		// Get the first primary or unique index (sorting primary keys first) that
		// is NOT a partial index.
		$sql = "
			SELECT indrelid, indkey
			FROM pg_catalog.pg_index
			WHERE indisunique AND indrelid=(
				SELECT oid FROM pg_catalog.pg_class
				WHERE relname='{$table}' AND relnamespace=(
					SELECT oid FROM pg_catalog.pg_namespace
					WHERE nspname='{$c_schema}'
				)
			) AND indpred IS NULL AND indexprs IS NULL
			ORDER BY indisprimary DESC LIMIT 1";
		$rs = $this->selectSet($sql);

		// If none, check for an OID column.  Even though OIDs can be duplicated, the edit and delete row
		// functions check that they're only modiying a single row.  Otherwise, return empty array.
		if ($rs->recordCount() == 0) {
			// Check for OID column
			$temp = array();
			if ($this->hasObjectID($table)) {
				$temp = array('oid');
			}
			$this->endTransaction();
			return $temp;
		}
		// Otherwise find the names of the keys
		else {
			$attnames = $this->getAttributeNames($oldtable, explode(' ', $rs->fields['indkey']));
			if (!is_array($attnames)) {
				$this->rollbackTransaction();
				return -1;
			}
			else {
				$this->endTransaction();
				return $attnames;
			}
		}
	}

	/**
	 * Adds a new row to a table
	 * @param $table The table in which to insert
	 * @param $fields Array of given field in values
	 * @param $values Array of new values for the row
	 * @param $nulls An array mapping column => something if it is to be null
	 * @param $format An array of the data type (VALUE or EXPRESSION)
	 * @param $types An array of field types
	 * @return 0 success
	 * @return -1 invalid parameters
	 */
	function insertRow($table, $fields, $values, $nulls, $format, $types) {

		if (!is_array($fields) || !is_array($values) || !is_array($nulls)
			|| !is_array($format) || !is_array($types)
			|| (count($fields) != count($values))
		) {
			return -1;
		}
		else {
			// Build clause
			if (count($values) > 0) {
				// Escape all field names
				$fields = array_map(array('Postgres','fieldClean'), $fields);
				$f_schema = $this->_schema;
				$this->fieldClean($table);
				$this->fieldClean($f_schema);

				$sql = '';
				foreach($values as $i => $value) {

					// Handle NULL values
					if (isset($nulls[$i]))
						$sql .= ',NULL';
					else
						$sql .= ',' . $this->formatValue($types[$i], $format[$i], $value);
				}

				$sql = "INSERT INTO \"{$f_schema}\".\"{$table}\" (\"". implode('","', $fields) ."\")
					VALUES (". substr($sql, 1) .")";

				return $this->execute($sql);
			}
		}

		return -1;
	}

	/**
	 * Updates a row in a table
	 * @param $table The table in which to update
	 * @param $vars An array mapping new values for the row
	 * @param $nulls An array mapping column => something if it is to be null
	 * @param $format An array of the data type (VALUE or EXPRESSION)
	 * @param $types An array of field types
	 * @param $keyarr An array mapping column => value to update
	 * @return 0 success
	 * @return -1 invalid parameters
	 */
	function editRow($table, $vars, $nulls, $format, $types, $keyarr) {
		if (!is_array($vars) || !is_array($nulls) || !is_array($format) || !is_array($types))
			return -1;
		else {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			$this->fieldClean($table);

			// Build clause
			if (sizeof($vars) > 0) {

				foreach($vars as $key => $value) {
					$this->fieldClean($key);

					// Handle NULL values
					if (isset($nulls[$key])) $tmp = 'NULL';
					else $tmp = $this->formatValue($types[$key], $format[$key], $value);

					if (isset($sql)) $sql .= ", \"{$key}\"={$tmp}";
					else $sql = "UPDATE \"{$f_schema}\".\"{$table}\" SET \"{$key}\"={$tmp}";
				}
				$first = true;
				foreach ($keyarr as $k => $v) {
					$this->fieldClean($k);
					$this->clean($v);
					if ($first) {
						$sql .= " WHERE \"{$k}\"='{$v}'";
						$first = false;
					}
					else $sql .= " AND \"{$k}\"='{$v}'";
				}
		}

			// Begin transaction.  We do this so that we can ensure only one row is
			// edited
			$status = $this->beginTransaction();
		if ($status != 0) {
			$this->rollbackTransaction();
				return -1;
		}

	   	$status = $this->execute($sql);
			if ($status != 0) { // update failed
			$this->rollbackTransaction();
				return -1;
			} elseif ($this->conn->Affected_Rows() != 1) { // more than one row could be updated
				$this->rollbackTransaction();
				return -2;
		}

			// End transaction
		return $this->endTransaction();
	}
	}

	/**
	 * Delete a row from a table
	 * @param $table The table from which to delete
	 * @param $key An array mapping column => value to delete
	 * @return 0 success
	 */
	function deleteRow($table, $key, $schema=false) {
		if (!is_array($key)) return -1;
		else {
			// Begin transaction.  We do this so that we can ensure only one row is
			// deleted
			$status = $this->beginTransaction();
			if ($status != 0) {
				$this->rollbackTransaction();
				return -1;
			}
			
			if ($schema === false) $schema = $this->_schema;

			$status = $this->delete($table, $key, $schema);
			if ($status != 0 || $this->conn->Affected_Rows() != 1) {
				$this->rollbackTransaction();
				return -2;
			}

			// End transaction
			return $this->endTransaction();
		}
	}

	// Sequence functions

	/**
	 * Returns properties of a single sequence
	 * @param $sequence Sequence name
	 * @return A recordset
	 */
	function getSequence($sequence) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$c_sequence = $sequence;
		$this->fieldClean($sequence);
		$this->clean($c_sequence);

        $sql = "
            SELECT
                c.relname AS seqname, s.*, 
                m.seqstart AS start_value, m.seqincrement AS increment_by, m.seqmax AS max_value, m.seqmin AS min_value, 
                m.seqcache AS cache_value, m.seqcycle AS is_cycled,  
			    pg_catalog.obj_description(m.seqrelid, 'pg_class') AS seqcomment,
				u.usename AS seqowner, n.nspname
            FROM
                \"{$sequence}\" AS s, pg_catalog.pg_sequence m,  
                pg_catalog.pg_class c, pg_catalog.pg_user u, pg_catalog.pg_namespace n                       
            WHERE
                c.relowner=u.usesysid AND c.relnamespace=n.oid 
                AND c.oid = m.seqrelid AND c.relname = '{$c_sequence}' AND c.relkind = 'S' AND n.nspname='{$c_schema}' 
                AND n.oid = c.relnamespace"; 

		return $this->selectSet( $sql );
	}

	/**
	 * Returns all sequences in the current database
	 * @return A recordset
	 */
	function getSequences($all = false) {
		if ($all) {
			// Exclude pg_catalog and information_schema tables
			$sql = "SELECT n.nspname, c.relname AS seqname, u.usename AS seqowner
				FROM pg_catalog.pg_class c, pg_catalog.pg_user u, pg_catalog.pg_namespace n
				WHERE c.relowner=u.usesysid AND c.relnamespace=n.oid
				AND c.relkind = 'S'
				AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
				ORDER BY nspname, seqname";
		} else {
			$c_schema = $this->_schema;
			$this->clean($c_schema);
			$sql = "SELECT c.relname AS seqname, u.usename AS seqowner, pg_catalog.obj_description(c.oid, 'pg_class') AS seqcomment,
				(SELECT spcname FROM pg_catalog.pg_tablespace pt WHERE pt.oid=c.reltablespace) AS tablespace
				FROM pg_catalog.pg_class c, pg_catalog.pg_user u, pg_catalog.pg_namespace n
				WHERE c.relowner=u.usesysid AND c.relnamespace=n.oid
				AND c.relkind = 'S' AND n.nspname='{$c_schema}' ORDER BY seqname";
		}

		return $this->selectSet( $sql );
	}

	/**
	 * Execute nextval on a given sequence
	 * @param $sequence Sequence name
	 * @return 0 success
	 * @return -1 sequence not found
	 */
	function nextvalSequence($sequence) {
		/* This double-cleaning is deliberate */
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->clean($f_schema);
		$this->fieldClean($sequence);
		$this->clean($sequence);

		$sql = "SELECT pg_catalog.NEXTVAL('\"{$f_schema}\".\"{$sequence}\"')";

		return $this->execute($sql);
	}

	/**
	 * Execute setval on a given sequence
	 * @param $sequence Sequence name
	 * @param $nextvalue The next value
	 * @return 0 success
	 * @return -1 sequence not found
	 */
	function setvalSequence($sequence, $nextvalue) {
		/* This double-cleaning is deliberate */
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->clean($f_schema);
		$this->fieldClean($sequence);
		$this->clean($sequence);
		$this->clean($nextvalue);

		$sql = "SELECT pg_catalog.SETVAL('\"{$f_schema}\".\"{$sequence}\"', '{$nextvalue}')";

		return $this->execute($sql);
	}

	/**
	 * Restart a given sequence to its start value
	 * @param $sequence Sequence name
	 * @return 0 success
	 * @return -1 sequence not found
	 */
	function restartSequence($sequence) {

		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($sequence);

		$sql = "ALTER SEQUENCE \"{$f_schema}\".\"{$sequence}\" RESTART;";

		return $this->execute($sql);
	}

	/**
	 * Resets a given sequence to min value of sequence
	 * @param $sequence Sequence name
	 * @return 0 success
	 * @return -1 sequence not found
	 */
	function resetSequence($sequence) {
		// Get the minimum value of the sequence
		$seq = $this->getSequence($sequence);
		if ($seq->recordCount() != 1) return -1;
		$minvalue = $seq->fields['min_value'];

		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		/* This double-cleaning is deliberate */
		$this->fieldClean($sequence);
		$this->clean($sequence);

		$sql = "SELECT pg_catalog.SETVAL('\"{$f_schema}\".\"{$sequence}\"', {$minvalue})";

		return $this->execute($sql);
	}

	/**
	 * Creates a new sequence
	 * @param $sequence Sequence name
	 * @param $increment The increment
	 * @param $minvalue The min value
	 * @param $maxvalue The max value
	 * @param $startvalue The starting value
	 * @param $cachevalue The cache value
	 * @param $cycledvalue True if cycled, false otherwise
	 * @return 0 success
	 */
	function createSequence($sequence, $increment, $minvalue, $maxvalue,
								$startvalue, $cachevalue, $cycledvalue) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($sequence);
		$this->clean($increment);
		$this->clean($minvalue);
		$this->clean($maxvalue);
		$this->clean($startvalue);
		$this->clean($cachevalue);

		$sql = "CREATE SEQUENCE \"{$f_schema}\".\"{$sequence}\"";
		if ($increment != '') $sql .= " INCREMENT {$increment}";
		if ($minvalue != '') $sql .= " MINVALUE {$minvalue}";
		if ($maxvalue != '') $sql .= " MAXVALUE {$maxvalue}";
		if ($startvalue != '') $sql .= " START {$startvalue}";
		if ($cachevalue != '') $sql .= " CACHE {$cachevalue}";
		if ($cycledvalue) $sql .= " CYCLE";

		return $this->execute($sql);
	}

	/**
	 * Rename a sequence
	 * @param $seqrs The sequence RecordSet returned by getSequence()
	 * @param $name The new name for the sequence
	 * @return 0 success
	 */
	function alterSequenceName($seqrs, $name) {
		/* vars are cleaned in _alterSequence */
		if (!empty($name) && ($seqrs->fields['seqname'] != $name)) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			$sql = "ALTER SEQUENCE \"{$f_schema}\".\"{$seqrs->fields['seqname']}\" RENAME TO \"{$name}\"";
			$status = $this->execute($sql);
			if ($status == 0)
				$seqrs->fields['seqname'] = $name;
			else
				return $status;
		}
		return 0;
	}

	/**
	 * Alter a sequence's owner
	 * @param $seqrs The sequence RecordSet returned by getSequence()
	 * @param $name The new owner for the sequence
	 * @return 0 success
	 */
	function alterSequenceOwner($seqrs, $owner) {
		// If owner has been changed, then do the alteration.  We are
		// careful to avoid this generally as changing owner is a
		// superuser only function.
		/* vars are cleaned in _alterSequence */
		if (!empty($owner) && ($seqrs->fields['seqowner'] != $owner)) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			$sql = "ALTER SEQUENCE \"{$f_schema}\".\"{$seqrs->fields['seqname']}\" OWNER TO \"{$owner}\"";
			return $this->execute($sql);
		}
		return 0;
	}

	/**
	 * Alter a sequence's schema
	 * @param $seqrs The sequence RecordSet returned by getSequence()
	 * @param $name The new schema for the sequence
	 * @return 0 success
	 */
	function alterSequenceSchema($seqrs, $schema) {
		/* vars are cleaned in _alterSequence */
		if (!empty($schema) && ($seqrs->fields['nspname'] != $schema)) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			$sql = "ALTER SEQUENCE \"{$f_schema}\".\"{$seqrs->fields['seqname']}\" SET SCHEMA {$schema}";
			return $this->execute($sql);
		}
		return 0;
	}

	/**
	 * Alter a sequence's properties
	 * @param $seqrs The sequence RecordSet returned by getSequence()
	 * @param $increment The sequence incremental value
	 * @param $minvalue The sequence minimum value
	 * @param $maxvalue The sequence maximum value
	 * @param $restartvalue The sequence current value
	 * @param $cachevalue The sequence cache value
	 * @param $cycledvalue Sequence can cycle ?
	 * @param $startvalue The sequence start value when issueing a restart
	 * @return 0 success
	 */
	function alterSequenceProps($seqrs, $increment,	$minvalue, $maxvalue,
								$restartvalue, $cachevalue, $cycledvalue, $startvalue) {

		$sql = '';
		/* vars are cleaned in _alterSequence */
		if (!empty($increment) && ($increment != $seqrs->fields['increment_by'])) $sql .= " INCREMENT {$increment}";
		if (!empty($minvalue) && ($minvalue != $seqrs->fields['min_value'])) $sql .= " MINVALUE {$minvalue}";
		if (!empty($maxvalue) && ($maxvalue != $seqrs->fields['max_value'])) $sql .= " MAXVALUE {$maxvalue}";
		if (!empty($restartvalue) && ($restartvalue != $seqrs->fields['last_value'])) $sql .= " RESTART {$restartvalue}";
		if (!empty($cachevalue) && ($cachevalue != $seqrs->fields['cache_value'])) $sql .= " CACHE {$cachevalue}";
		if (!empty($startvalue) && ($startvalue != $seqrs->fields['start_value'])) $sql .= " START {$startvalue}";
		// toggle cycle yes/no
		if (!is_null($cycledvalue))	$sql .= (!$cycledvalue ? ' NO ' : '') . " CYCLE";
		if ($sql != '') {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			$sql = "ALTER SEQUENCE \"{$f_schema}\".\"{$seqrs->fields['seqname']}\" {$sql}";
			return $this->execute($sql);
		}
		return 0;
	}

	/**
	 * Protected method which alter a sequence
	 * SHOULDN'T BE CALLED OUTSIDE OF A TRANSACTION
	 * @param $seqrs The sequence recordSet returned by getSequence()
	 * @param $name The new name for the sequence
	 * @param $comment The comment on the sequence
	 * @param $owner The new owner for the sequence
	 * @param $schema The new schema for the sequence
	 * @param $increment The increment
	 * @param $minvalue The min value
	 * @param $maxvalue The max value
	 * @param $restartvalue The starting value
	 * @param $cachevalue The cache value
	 * @param $cycledvalue True if cycled, false otherwise
	 * @param $startvalue The sequence start value when issueing a restart
	 * @return 0 success
	 * @return -3 rename error
	 * @return -4 comment error
	 * @return -5 owner error
	 * @return -6 get sequence props error
	 * @return -7 schema error
	 */
	protected
	function _alterSequence($seqrs, $name, $comment, $owner, $schema, $increment,
	$minvalue, $maxvalue, $restartvalue, $cachevalue, $cycledvalue, $startvalue) {

		$this->fieldArrayClean($seqrs->fields);

		// Comment
		$status = $this->setComment('SEQUENCE', $seqrs->fields['seqname'], '', $comment);
		if ($status != 0)
			return -4;

		// Owner
		$this->fieldClean($owner);
		$status = $this->alterSequenceOwner($seqrs, $owner);
		if ($status != 0)
			return -5;

		// Props
		$this->clean($increment);
		$this->clean($minvalue);
		$this->clean($maxvalue);
		$this->clean($restartvalue);
		$this->clean($cachevalue);
		$this->clean($cycledvalue);
		$this->clean($startvalue);
		$status = $this->alterSequenceProps($seqrs, $increment,	$minvalue,
			$maxvalue, $restartvalue, $cachevalue, $cycledvalue, $startvalue);
		if ($status != 0)
			return -6;

		// Rename
		$this->fieldClean($name);
		$status = $this->alterSequenceName($seqrs, $name);
		if ($status != 0)
			return -3;

		// Schema
		$this->clean($schema);
		$status = $this->alterSequenceSchema($seqrs, $schema);
		if ($status != 0)
			return -7;

		return 0;
	}

	/**
	 * Alters a sequence
	 * @param $sequence The name of the sequence
	 * @param $name The new name for the sequence
	 * @param $comment The comment on the sequence
	 * @param $owner The new owner for the sequence
	 * @param $schema The new schema for the sequence
	 * @param $increment The increment
	 * @param $minvalue The min value
	 * @param $maxvalue The max value
	 * @param $restartvalue The starting value
	 * @param $cachevalue The cache value
	 * @param $cycledvalue True if cycled, false otherwise
	 * @param $startvalue The sequence start value when issueing a restart
	 * @return 0 success
	 * @return -1 transaction error
	 * @return -2 get existing sequence error
	 * @return $this->_alterSequence error code
	 */
    function alterSequence($sequence, $name, $comment, $owner=null, $schema=null, $increment=null,
	$minvalue=null, $maxvalue=null, $restartvalue=null, $cachevalue=null, $cycledvalue=null, $startvalue=null) {

		$this->fieldClean($sequence);

		$data = $this->getSequence($sequence);

		if ($data->recordCount() != 1)
			return -2;

		$status = $this->beginTransaction();
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		$status = $this->_alterSequence($data, $name, $comment, $owner, $schema, $increment,
				$minvalue, $maxvalue, $restartvalue, $cachevalue, $cycledvalue, $startvalue);

		if ($status != 0) {
			$this->rollbackTransaction();
			return $status;
		}

		return $this->endTransaction();
	}

	/**
	 * Drops a given sequence
	 * @param $sequence Sequence name
	 * @param $cascade True to cascade drop, false to restrict
	 * @return 0 success
	 */
	function dropSequence($sequence, $cascade) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($sequence);

		$sql = "DROP SEQUENCE \"{$f_schema}\".\"{$sequence}\"";
		if ($cascade) $sql .= " CASCADE";

		return $this->execute($sql);
	}

	// View functions

	/**
	 * Returns all details for a particular view
	 * @param $view The name of the view to retrieve
	 * @return View info
	 */
	function getView($view) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($view);

		$sql = "
			SELECT c.relname, n.nspname, pg_catalog.pg_get_userbyid(c.relowner) AS relowner,
				pg_catalog.pg_get_viewdef(c.oid, true) AS vwdefinition,
				pg_catalog.obj_description(c.oid, 'pg_class') AS relcomment
			FROM pg_catalog.pg_class c
				LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
			WHERE (c.relname = '{$view}') AND n.nspname='{$c_schema}'";

		return $this->selectSet($sql);
	}

	/**
	 * Returns a list of all views in the database
	 * @return All views
	 */
	function getViews() {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$sql = "
			SELECT c.relname, pg_catalog.pg_get_userbyid(c.relowner) AS relowner,
				pg_catalog.obj_description(c.oid, 'pg_class') AS relcomment
			FROM pg_catalog.pg_class c
				LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
			WHERE (n.nspname='{$c_schema}') AND (c.relkind = 'v'::\"char\")
			ORDER BY relname";

		return $this->selectSet($sql);
	}

	/**
	 * Updates a view.
	 * @param $viewname The name fo the view to update
	 * @param $definition The new definition for the view
	 * @return 0 success
	 * @return -1 transaction error
	 * @return -2 drop view error
	 * @return -3 create view error
	 */
	function setView($viewname, $definition,$comment) {
		return $this->createView($viewname, $definition, true, $comment);
	}

	/**
	 * Creates a new view.
	 * @param $viewname The name of the view to create
	 * @param $definition The definition for the new view
	 * @param $replace True to replace the view, false otherwise
	 * @return 0 success
	 */
	function createView($viewname, $definition, $replace, $comment) {
		$status = $this->beginTransaction();
		if ($status != 0) return -1;

		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($viewname);

		// Note: $definition not cleaned

		$sql = "CREATE ";
		if ($replace) $sql .= "OR REPLACE ";
		$sql .= "VIEW \"{$f_schema}\".\"{$viewname}\" AS {$definition}";

		$status = $this->execute($sql);
		if ($status) {
			$this->rollbackTransaction();
			return -1;
		}

		if ($comment != '') {
			$status = $this->setComment('VIEW', $viewname, '', $comment);
			if ($status) {
				$this->rollbackTransaction();
			return -1;
			}
		}

		return $this->endTransaction();
	}

	/**
	 * Rename a view
	 * @param $vwrs The view recordSet returned by getView()
	 * @param $name The new view's name
	 * @return 0 success
	 */
	function alterViewName($vwrs, $name) {
		// Rename (only if name has changed)
		/* $vwrs and $name are cleaned in _alterView */
		if (!empty($name) && ($name != $vwrs->fields['relname'])) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			$sql = "ALTER VIEW \"{$f_schema}\".\"{$vwrs->fields['relname']}\" RENAME TO \"{$name}\"";
			$status =  $this->execute($sql);
			if ($status == 0)
				$vwrs->fields['relname'] = $name;
			else
				return $status;
		}
		return 0;
	}

	/**
	 * Alter a view's owner
	 * @param $vwrs The view recordSet returned by getView()
	 * @param $name The new view's owner
	 * @return 0 success
	 */
	function alterViewOwner($vwrs, $owner = null) {
		/* $vwrs and $owner are cleaned in _alterView */
		if ((!empty($owner)) && ($vwrs->fields['relowner'] != $owner)) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			// If owner has been changed, then do the alteration.  We are
			// careful to avoid this generally as changing owner is a
			// superuser only function.
			$sql = "ALTER TABLE \"{$f_schema}\".\"{$vwrs->fields['relname']}\" OWNER TO \"{$owner}\"";
			return $this->execute($sql);
		}
		return 0;
		}

	/**
	 * Alter a view's schema
	 * @param $vwrs The view recordSet returned by getView()
	 * @param $name The new view's schema
	 * @return 0 success
	 */
	function alterViewSchema($vwrs, $schema) {
		/* $vwrs and $schema are cleaned in _alterView */
		if (!empty($schema) && ($vwrs->fields['nspname'] != $schema)) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			// If tablespace has been changed, then do the alteration.  We
			// don't want to do this unnecessarily.
			$sql = "ALTER TABLE \"{$f_schema}\".\"{$vwrs->fields['relname']}\" SET SCHEMA \"{$schema}\"";
			return $this->execute($sql);
		}
		return 0;
	}

	 /**
	  * Protected method which alter a view
	  * SHOULDN'T BE CALLED OUTSIDE OF A TRANSACTION
	  * @param $vwrs The view recordSet returned by getView()
	  * @param $name The new name for the view
	  * @param $owner The new owner for the view
	  * @param $comment The comment on the view
	  * @return 0 success
	  * @return -3 rename error
	  * @return -4 comment error
	  * @return -5 owner error
	  * @return -6 schema error
	  */
	protected
    function _alterView($vwrs, $name, $owner, $schema, $comment) {

    	$this->fieldArrayClean($vwrs->fields);

		// Comment
		if ($this->setComment('VIEW', $vwrs->fields['relname'], '', $comment) != 0)
			return -4;

		// Owner
		$this->fieldClean($owner);
		$status = $this->alterViewOwner($vwrs, $owner);
		if ($status != 0) return -5;

		// Rename
		$this->fieldClean($name);
		$status = $this->alterViewName($vwrs, $name);
		if ($status != 0) return -3;

		// Schema
		$this->fieldClean($schema);
		$status = $this->alterViewSchema($vwrs, $schema);
		if ($status != 0) return -6;

		return 0;
	}

	/**
	 * Alter view properties
	 * @param $view The name of the view
	 * @param $name The new name for the view
	 * @param $owner The new owner for the view
	 * @param $schema The new schema for the view
	 * @param $comment The comment on the view
	 * @return 0 success
	 * @return -1 transaction error
	 * @return -2 get existing view error
	 * @return $this->_alterView error code
	 */
	function alterView($view, $name, $owner, $schema, $comment) {

		$data = $this->getView($view);
		if ($data->recordCount() != 1)
			return -2;

		$status = $this->beginTransaction();
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		$status = $this->_alterView($data, $name, $owner, $schema, $comment);

		if ($status != 0) {
			$this->rollbackTransaction();
			return $status;
		}

		return $this->endTransaction();
	}

	/**
	 * Drops a view.
	 * @param $viewname The name of the view to drop
	 * @param $cascade True to cascade drop, false to restrict
	 * @return 0 success
	 */
	function dropView($viewname, $cascade) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($viewname);

		$sql = "DROP VIEW \"{$f_schema}\".\"{$viewname}\"";
		if ($cascade) $sql .= " CASCADE";

		return $this->execute($sql);
	}

	// Index functions

	/**
	 * Grabs a list of indexes for a table
	 * @param $table The name of a table whose indexes to retrieve
	 * @param $unique Only get unique/pk indexes
	 * @return A recordset
	 */
	function getIndexes($table = '', $unique = false) {
		$this->clean($table);

		$sql = "
			SELECT c2.relname AS indname, i.indisprimary, i.indisunique, i.indisclustered,
				pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) AS inddef
			FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
			WHERE c.relname = '{$table}' AND pg_catalog.pg_table_is_visible(c.oid)
				AND c.oid = i.indrelid AND i.indexrelid = c2.oid
		";
		if ($unique) $sql .= " AND i.indisunique ";
		$sql .= " ORDER BY c2.relname";

		return $this->selectSet($sql);
	}

	/** 
	 * test if a table has been clustered on an index
	 * @param $table The table to test
	 * 
	 * @return true if the table has been already clustered
	 */
	function alreadyClustered($table) {
		
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);

		$sql = "SELECT i.indisclustered
			FROM pg_catalog.pg_class c, pg_catalog.pg_index i
			WHERE c.relname = '{$table}'
				AND c.oid = i.indrelid AND i.indisclustered
				AND c.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace
					WHERE nspname='{$c_schema}')
				";
		
		$v = $this->selectSet($sql);
		
		if ($v->recordCount() == 0)
			return false;
			
		return true;
	}
	
	/**
	 * Creates an index
	 * @param $name The index name
	 * @param $table The table on which to add the index
	 * @param $columns An array of columns that form the index
	 *                 or a string expression for a functional index
	 * @param $type The index type
	 * @param $unique True if unique, false otherwise
	 * @param $where Index predicate ('' for none)
	 * @param $tablespace The tablespaces ('' means none/default)
	 * @return 0 success
	 */
	function createIndex($name, $table, $columns, $type, $unique, $where, $tablespace, $concurrently) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($name);
		$this->fieldClean($table);

		$sql = "CREATE";
		if ($unique) $sql .= " UNIQUE";
		$sql .= " INDEX";
		if ($concurrently) $sql .= " CONCURRENTLY";
		$sql .= " \"{$name}\" ON \"{$f_schema}\".\"{$table}\" USING {$type} ";

		if (is_array($columns)) {
			$this->arrayClean($columns);
			$sql .= "(\"" . implode('","', $columns) . "\")";
		} else {
			$sql .= "(" . $columns .")";
		}

		// Tablespace
		if ($this->hasTablespaces() && $tablespace != '') {
			$this->fieldClean($tablespace);
			$sql .= " TABLESPACE \"{$tablespace}\"";
		}

		// Predicate
		if (trim($where) != '') {
			$sql .= " WHERE ({$where})";
		}

		return $this->execute($sql);
	}

	/**
	 * Removes an index from the database
	 * @param $index The index to drop
	 * @param $cascade True to cascade drop, false to restrict
	 * @return 0 success
	 */
	function dropIndex($index, $cascade) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($index);

		$sql = "DROP INDEX \"{$f_schema}\".\"{$index}\"";
		if ($cascade) $sql .= " CASCADE";

		return $this->execute($sql);
	}

	/**
	 * Rebuild indexes
	 * @param $type 'DATABASE' or 'TABLE' or 'INDEX'
	 * @param $name The name of the specific database, table, or index to be reindexed
	 * @param $force If true, recreates indexes forcedly in PostgreSQL 7.0-7.1, forces rebuild of system indexes in 7.2-7.3, ignored in >=7.4
	 */
	function reindex($type, $name, $force = false) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($name);
		switch($type) {
			case 'DATABASE':
				$sql = "REINDEX {$type} \"{$name}\"";
				if ($force) $sql .= ' FORCE';
				break;
			case 'TABLE':
			case 'INDEX':
				$sql = "REINDEX {$type} \"{$f_schema}\".\"{$name}\"";
				if ($force) $sql .= ' FORCE';
				break;
			default:
				return -1;
	}

		return $this->execute($sql);
	}

	/**
	 * Clusters an index
	 * @param $index The name of the index
	 * @param $table The table the index is on
	 * @return 0 success
	 */
	function clusterIndex($table='', $index='') {
		
		$sql = 'CLUSTER';
		
		// We don't bother with a transaction here, as there's no point rolling
		// back an expensive cluster if a cheap analyze fails for whatever reason
		
		if (!empty($table)) {
			$f_schema = $this->_schema;
			$this->fieldClean($f_schema);
			$this->fieldClean($table);
			$sql .= " \"{$f_schema}\".\"{$table}\"";
			
			if (!empty($index)) {
				$this->fieldClean($index);
				$sql .= " USING \"{$index}\"";
			}
		}

		return $this->execute($sql);
	}

	// Constraint functions

	/**
	 * Returns a list of all constraints on a table
	 * @param $table The table to find rules for
	 * @return A recordset
	 */
	function getConstraints($table) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);

		// This SQL is greatly complicated by the need to retrieve
		// index clustering information for primary and unique constraints
		$sql = "SELECT
				pc.conname,
				pg_catalog.pg_get_constraintdef(pc.oid, true) AS consrc,
				pc.contype,
				CASE WHEN pc.contype='u' OR pc.contype='p' THEN (
					SELECT
						indisclustered
					FROM
						pg_catalog.pg_depend pd,
						pg_catalog.pg_class pl,
						pg_catalog.pg_index pi
					WHERE
						pd.refclassid=pc.tableoid
						AND pd.refobjid=pc.oid
						AND pd.objid=pl.oid
						AND pl.oid=pi.indexrelid
				) ELSE
					NULL
				END AS indisclustered
			FROM
				pg_catalog.pg_constraint pc
			WHERE
				pc.conrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='{$table}'
					AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace
					WHERE nspname='{$c_schema}'))
			ORDER BY
				1
		";

		return $this->selectSet($sql);
	}

	/**
	 * Returns a list of all constraints on a table,
	 * including constraint name, definition, related col and referenced namespace,
	 * table and col if needed
	 * @param $table the table where we are looking for fk
	 * @return a recordset
	 */
	function getConstraintsWithFields($table) {

		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($table);

		// get the max number of col used in a constraint for the table
		$sql = "SELECT DISTINCT
			max(SUBSTRING(array_dims(c.conkey) FROM  \$patern\$^\\[.*:(.*)\\]$\$patern\$)) as nb
		FROM pg_catalog.pg_constraint AS c
			JOIN pg_catalog.pg_class AS r ON (c.conrelid=r.oid)
			JOIN pg_catalog.pg_namespace AS ns ON (r.relnamespace=ns.oid)
		WHERE
			r.relname = '{$table}' AND ns.nspname='{$c_schema}'";

		$rs = $this->selectSet($sql);

		if ($rs->EOF) $max_col = 0;
		else $max_col = $rs->fields['nb'];

		$sql = '
			SELECT
				c.oid AS conid, c.contype, c.conname, pg_catalog.pg_get_constraintdef(c.oid, true) AS consrc,
				ns1.nspname as p_schema, r1.relname as p_table, ns2.nspname as f_schema,
				r2.relname as f_table, f1.attname as p_field, f1.attnum AS p_attnum, f2.attname as f_field,
				f2.attnum AS f_attnum, pg_catalog.obj_description(c.oid, \'pg_constraint\') AS constcomment,
				c.conrelid, c.confrelid
			FROM
				pg_catalog.pg_constraint AS c
				JOIN pg_catalog.pg_class AS r1 ON (c.conrelid=r1.oid)
				JOIN pg_catalog.pg_attribute AS f1 ON (f1.attrelid=r1.oid AND (f1.attnum=c.conkey[1]';
		for ($i = 2; $i <= $rs->fields['nb']; $i++) {
			$sql.= " OR f1.attnum=c.conkey[$i]";
		}
		$sql.= '))
				JOIN pg_catalog.pg_namespace AS ns1 ON r1.relnamespace=ns1.oid
				LEFT JOIN (
					pg_catalog.pg_class AS r2 JOIN pg_catalog.pg_namespace AS ns2 ON (r2.relnamespace=ns2.oid)
				) ON (c.confrelid=r2.oid)
				LEFT JOIN pg_catalog.pg_attribute AS f2 ON
					(f2.attrelid=r2.oid AND ((c.confkey[1]=f2.attnum AND c.conkey[1]=f1.attnum)';
		for ($i = 2; $i <= $rs->fields['nb']; $i++)
			$sql.= " OR (c.confkey[$i]=f2.attnum AND c.conkey[$i]=f1.attnum)";

		$sql .= sprintf("))
			WHERE
				r1.relname = '%s' AND ns1.nspname='%s'
			ORDER BY 1", $table, $c_schema);

		return $this->selectSet($sql);
	}

	/**
	 * Adds a primary key constraint to a table
	 * @param $table The table to which to add the primery key
	 * @param $fields (array) An array of fields over which to add the primary key
	 * @param $name (optional) The name to give the key, otherwise default name is assigned
	 * @param $tablespace (optional) The tablespace for the schema, '' indicates default.
	 * @return 0 success
	 * @return -1 no fields given
	 */
	function addPrimaryKey($table, $fields, $name = '', $tablespace = '') {
		if (!is_array($fields) || sizeof($fields) == 0) return -1;
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldArrayClean($fields);
		$this->fieldClean($name);
		$this->fieldClean($tablespace);

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ADD ";
		if ($name != '') $sql .= "CONSTRAINT \"{$name}\" ";
		$sql .= "PRIMARY KEY (\"" . join('","', $fields) . "\")";

		if ($tablespace != '' && $this->hasTablespaces())
			$sql .= " USING INDEX TABLESPACE \"{$tablespace}\"";

		return $this->execute($sql);
	}

	/**
	 * Adds a unique constraint to a table
	 * @param $table The table to which to add the unique key
	 * @param $fields (array) An array of fields over which to add the unique key
	 * @param $name (optional) The name to give the key, otherwise default name is assigned
	 * @param $tablespace (optional) The tablespace for the schema, '' indicates default.
	 * @return 0 success
	 * @return -1 no fields given
	 */
	function addUniqueKey($table, $fields, $name = '', $tablespace = '') {
		if (!is_array($fields) || sizeof($fields) == 0) return -1;
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldArrayClean($fields);
		$this->fieldClean($name);
		$this->fieldClean($tablespace);

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ADD ";
		if ($name != '') $sql .= "CONSTRAINT \"{$name}\" ";
		$sql .= "UNIQUE (\"" . join('","', $fields) . "\")";

		if ($tablespace != '' && $this->hasTablespaces())
			$sql .= " USING INDEX TABLESPACE \"{$tablespace}\"";

		return $this->execute($sql);
	}

	/**
	 * Adds a check constraint to a table
	 * @param $table The table to which to add the check
	 * @param $definition The definition of the check
	 * @param $name (optional) The name to give the check, otherwise default name is assigned
	 * @return 0 success
	 */
	function addCheckConstraint($table, $definition, $name = '') {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldClean($name);
		// @@ How the heck do you clean a definition???

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ADD ";
		if ($name != '') $sql .= "CONSTRAINT \"{$name}\" ";
		$sql .= "CHECK ({$definition})";

		return $this->execute($sql);
	}

	/**
	 * Drops a check constraint from a table
	 * @param $table The table from which to drop the check
	 * @param $name The name of the check to be dropped
	 * @return 0 success
	 * @return -2 transaction error
	 * @return -3 lock error
	 * @return -4 check drop error
	 */
	function dropCheckConstraint($table, $name) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$c_table = $table;
		$this->fieldClean($table);
		$this->clean($c_table);
		$this->clean($name);

		// Begin transaction
		$status = $this->beginTransaction();
		if ($status != 0) return -2;

		// Properly lock the table
		$sql = "LOCK TABLE \"{$f_schema}\".\"{$table}\" IN ACCESS EXCLUSIVE MODE";
		$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -3;
		}

		// Delete the check constraint
		$sql = "DELETE FROM pg_relcheck WHERE rcrelid=(SELECT oid FROM pg_catalog.pg_class WHERE relname='{$c_table}'
			AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE
			nspname = '{$c_schema}')) AND rcname='{$name}'";
	   	$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -4;
		}

		// Update the pg_class catalog to reflect the new number of checks
		$sql = "UPDATE pg_class SET relchecks=(SELECT COUNT(*) FROM pg_relcheck WHERE
					rcrelid=(SELECT oid FROM pg_catalog.pg_class WHERE relname='{$c_table}'
						AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE
						nspname = '{$c_schema}')))
					WHERE relname='{$c_table}'";
	   	$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -4;
		}

		// Otherwise, close the transaction
		return $this->endTransaction();
	}

	/**
	 * Adds a foreign key constraint to a table
	 * @param $targschema The schema that houses the target table to which to add the foreign key
	 * @param $targtable The table to which to add the foreign key
	 * @param $target The table that contains the target columns
	 * @param $sfields (array) An array of source fields over which to add the foreign key
	 * @param $tfields (array) An array of target fields over which to add the foreign key
	 * @param $upd_action The action for updates (eg. RESTRICT)
	 * @param $del_action The action for deletes (eg. RESTRICT)
	 * @param $match The match type (eg. MATCH FULL)
	 * @param $deferrable The deferrability (eg. NOT DEFERRABLE)
	 * @param $intially The initial deferrability (eg. INITIALLY IMMEDIATE)
	 * @param $name (optional) The name to give the key, otherwise default name is assigned
	 * @return 0 success
	 * @return -1 no fields given
	 */
	function addForeignKey($table, $targschema, $targtable, $sfields, $tfields, $upd_action, $del_action,
	$match, $deferrable, $initially, $name = '') {
		if (!is_array($sfields) || sizeof($sfields) == 0 ||
			!is_array($tfields) || sizeof($tfields) == 0) return -1;
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($table);
		$this->fieldClean($targschema);
		$this->fieldClean($targtable);
		$this->fieldArrayClean($sfields);
		$this->fieldArrayClean($tfields);
		$this->fieldClean($name);

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$table}\" ADD ";
		if ($name != '') $sql .= "CONSTRAINT \"{$name}\" ";
		$sql .= "FOREIGN KEY (\"" . join('","', $sfields) . "\") ";
		// Target table needs to be fully qualified
		$sql .= "REFERENCES \"{$targschema}\".\"{$targtable}\"(\"" . join('","', $tfields) . "\") ";
		if ($match != $this->fkmatches[0]) $sql .= " {$match}";
		if ($upd_action != $this->fkactions[0]) $sql .= " ON UPDATE {$upd_action}";
		if ($del_action != $this->fkactions[0]) $sql .= " ON DELETE {$del_action}";
		if ($deferrable != $this->fkdeferrable[0]) $sql .= " {$deferrable}";
		if ($initially != $this->fkinitial[0]) $sql .= " {$initially}";

		return $this->execute($sql);
	}

	/**
	 * Removes a constraint from a relation
	 * @param $constraint The constraint to drop
	 * @param $relation The relation from which to drop
	 * @param $type The type of constraint (c, f, u or p)
	 * @param $cascade True to cascade drop, false to restrict
	 * @return 0 success
	 */
	function dropConstraint($constraint, $relation, $type, $cascade) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($constraint);
		$this->fieldClean($relation);

		$sql = "ALTER TABLE \"{$f_schema}\".\"{$relation}\" DROP CONSTRAINT \"{$constraint}\"";
		if ($cascade) $sql .= " CASCADE";

		return $this->execute($sql);
	}

	/**
	 * A function for getting all columns linked by foreign keys given a group of tables
	 * @param $tables multi dimensional assoc array that holds schema and table name
	 * @return A recordset of linked tables and columns
	 * @return -1 $tables isn't an array
	 */
	function getLinkingKeys($tables) {
		if (!is_array($tables)) return -1;
		
		$this->clean($tables[0]['tablename']);
		$this->clean($tables[0]['schemaname']);
		$tables_list = "'{$tables[0]['tablename']}'";
		$schema_list = "'{$tables[0]['schemaname']}'";
		$schema_tables_list = "'{$tables[0]['schemaname']}.{$tables[0]['tablename']}'";

		for ($i = 1; $i < sizeof($tables); $i++) {
			$this->clean($tables[$i]['tablename']);
			$this->clean($tables[$i]['schemaname']);
			$tables_list .= ", '{$tables[$i]['tablename']}'";
			$schema_list .= ", '{$tables[$i]['schemaname']}'";
			$schema_tables_list .= ", '{$tables[$i]['schemaname']}.{$tables[$i]['tablename']}'";
		}

		$maxDimension = 1;

		$sql = "
			SELECT DISTINCT
				array_dims(pc.conkey) AS arr_dim,
				pgc1.relname AS p_table
			FROM
				pg_catalog.pg_constraint AS pc,
				pg_catalog.pg_class AS pgc1
			WHERE
				pc.contype = 'f'
				AND (pc.conrelid = pgc1.relfilenode OR pc.confrelid = pgc1.relfilenode)
				AND pgc1.relname IN ($tables_list)
			";

		//parse our output to find the highest dimension of foreign keys since pc.conkey is stored in an array
		$rs = $this->selectSet($sql);
		while (!$rs->EOF) {
			$arrData = explode(':', $rs->fields['arr_dim']);
			$tmpDimension = intval(substr($arrData[1], 0, strlen($arrData[1] - 1)));
			$maxDimension = $tmpDimension > $maxDimension ? $tmpDimension : $maxDimension;
			$rs->MoveNext();
		}

		//we know the highest index for foreign keys that conkey goes up to, expand for us in an IN query
		$cons_str = '( (pfield.attnum = conkey[1] AND cfield.attnum = confkey[1]) ';
		for ($i = 2; $i <= $maxDimension; $i++) {
			$cons_str .= "OR (pfield.attnum = conkey[{$i}] AND cfield.attnum = confkey[{$i}]) ";
		}
		$cons_str .= ') ';

		$sql = "
			SELECT
				pgc1.relname AS p_table,
				pgc2.relname AS f_table,
				pfield.attname AS p_field,
				cfield.attname AS f_field,
				pgns1.nspname AS p_schema,
				pgns2.nspname AS f_schema
			FROM
				pg_catalog.pg_constraint AS pc,
				pg_catalog.pg_class AS pgc1,
				pg_catalog.pg_class AS pgc2,
				pg_catalog.pg_attribute AS pfield,
				pg_catalog.pg_attribute AS cfield,
				(SELECT oid AS ns_id, nspname FROM pg_catalog.pg_namespace WHERE nspname IN ($schema_list) ) AS pgns1,
 				(SELECT oid AS ns_id, nspname FROM pg_catalog.pg_namespace WHERE nspname IN ($schema_list) ) AS pgns2
			WHERE
				pc.contype = 'f'
				AND pgc1.relnamespace = pgns1.ns_id
 				AND pgc2.relnamespace = pgns2.ns_id
				AND pc.conrelid = pgc1.relfilenode
				AND pc.confrelid = pgc2.relfilenode
				AND pfield.attrelid = pc.conrelid
				AND cfield.attrelid = pc.confrelid
				AND $cons_str
				AND pgns1.nspname || '.' || pgc1.relname IN ($schema_tables_list)
				AND pgns2.nspname || '.' || pgc2.relname IN ($schema_tables_list)
		";
		return $this->selectSet($sql);
	}

	/**
	 * Finds the foreign keys that refer to the specified table
	 * @param $table The table to find referrers for
	 * @return A recordset
	 */
	function getReferrers($table) {
		$this->clean($table);

		$status = $this->beginTransaction();
		if ($status != 0) return -1;

		$c_schema = $this->_schema;
		$this->clean($c_schema);

		$sql = "
			SELECT
				pn.nspname,
				pl.relname,
				pc.conname,
				pg_catalog.pg_get_constraintdef(pc.oid) AS consrc
			FROM
				pg_catalog.pg_constraint pc,
				pg_catalog.pg_namespace pn,
				pg_catalog.pg_class pl
			WHERE
				pc.connamespace = pn.oid
				AND pc.conrelid = pl.oid
				AND pc.contype = 'f'
				AND confrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='{$table}'
					AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace
					WHERE nspname='{$c_schema}'))
			ORDER BY 1,2,3
		";

		return $this->selectSet($sql);
		}

	// Domain functions

	/**
	 * Gets all information for a single domain
	 * @param $domain The name of the domain to fetch
	 * @return A recordset
	 */
	function getDomain($domain) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($domain);

		$sql = "
			SELECT
				t.typname AS domname,
				pg_catalog.format_type(t.typbasetype, t.typtypmod) AS domtype,
				t.typnotnull AS domnotnull,
				t.typdefault AS domdef,
				pg_catalog.pg_get_userbyid(t.typowner) AS domowner,
				pg_catalog.obj_description(t.oid, 'pg_type') AS domcomment
			FROM
				pg_catalog.pg_type t
			WHERE
				t.typtype = 'd'
				AND t.typname = '{$domain}'
				AND t.typnamespace = (SELECT oid FROM pg_catalog.pg_namespace
					WHERE nspname = '{$c_schema}')";

		return $this->selectSet($sql);
		}

	/**
	 * Return all domains in current schema.  Excludes domain constraints.
	 * @return All tables, sorted alphabetically
	 */
	function getDomains() {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		
		$sql = "
			SELECT
				t.typname AS domname,
				pg_catalog.format_type(t.typbasetype, t.typtypmod) AS domtype,
				t.typnotnull AS domnotnull,
				t.typdefault AS domdef,
				pg_catalog.pg_get_userbyid(t.typowner) AS domowner,
				pg_catalog.obj_description(t.oid, 'pg_type') AS domcomment
			FROM
				pg_catalog.pg_type t
			WHERE
				t.typtype = 'd'
				AND t.typnamespace = (SELECT oid FROM pg_catalog.pg_namespace
					WHERE nspname='{$c_schema}')
			ORDER BY t.typname";

		return $this->selectSet($sql);
	}

	/**
	 * Get domain constraints
	 * @param $domain The name of the domain whose constraints to fetch
	 * @return A recordset
	 */
	function getDomainConstraints($domain) {
		$c_schema = $this->_schema;
		$this->clean($c_schema);
		$this->clean($domain);

		$sql = "
			SELECT
				conname,
				contype,
				pg_catalog.pg_get_constraintdef(oid, true) AS consrc
			FROM
				pg_catalog.pg_constraint
			WHERE
				contypid = (
					SELECT oid FROM pg_catalog.pg_type
					WHERE typname='{$domain}'
						AND typnamespace = (
							SELECT oid FROM pg_catalog.pg_namespace
							WHERE nspname = '{$c_schema}')
				)
			ORDER BY conname";

		return $this->selectSet($sql);
	}

	/**
	 * Creates a domain
	 * @param $domain The name of the domain to create
	 * @param $type The base type for the domain
	 * @param $length Optional type length
	 * @param $array True for array type, false otherwise
	 * @param $notnull True for NOT NULL, false otherwise
	 * @param $default Default value for domain
	 * @param $check A CHECK constraint if there is one
	 * @return 0 success
	 */
	function createDomain($domain, $type, $length, $array, $notnull, $default, $check) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($domain);

		$sql = "CREATE DOMAIN \"{$f_schema}\".\"{$domain}\" AS ";

		if ($length == '')
			$sql .= $type;
		else {
			switch ($type) {
				// Have to account for weird placing of length for with/without
				// time zone types
				case 'timestamp with time zone':
				case 'timestamp without time zone':
					$qual = substr($type, 9);
					$sql .= "timestamp({$length}){$qual}";
					break;
				case 'time with time zone':
				case 'time without time zone':
					$qual = substr($type, 4);
					$sql .= "time({$length}){$qual}";
					break;
				default:
					$sql .= "{$type}({$length})";
			}
		}

		// Add array qualifier, if requested
		if ($array) $sql .= '[]';

		if ($notnull) $sql .= ' NOT NULL';
		if ($default != '') $sql .= " DEFAULT {$default}";
		if ($this->hasDomainConstraints() && $check != '') $sql .= " CHECK ({$check})";

		return $this->execute($sql);
	}

	/**
	 * Alters a domain
	 * @param $domain The domain to alter
	 * @param $domdefault The domain default
	 * @param $domnotnull True for NOT NULL, false otherwise
	 * @param $domowner The domain owner
	 * @return 0 success
	 * @return -1 transaction error
	 * @return -2 default error
	 * @return -3 not null error
	 * @return -4 owner error
	 */
	function alterDomain($domain, $domdefault, $domnotnull, $domowner) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($domain);
		$this->fieldClean($domowner);

		$status = $this->beginTransaction();
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		// Default
		if ($domdefault == '')
			$sql = "ALTER DOMAIN \"{$f_schema}\".\"{$domain}\" DROP DEFAULT";
		else
			$sql = "ALTER DOMAIN \"{$f_schema}\".\"{$domain}\" SET DEFAULT {$domdefault}";

		$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -2;
		}

		// NOT NULL
		if ($domnotnull)
			$sql = "ALTER DOMAIN \"{$f_schema}\".\"{$domain}\" SET NOT NULL";
		else
			$sql = "ALTER DOMAIN \"{$f_schema}\".\"{$domain}\" DROP NOT NULL";

		$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -3;
		}

		// Owner
		$sql = "ALTER DOMAIN \"{$f_schema}\".\"{$domain}\" OWNER TO \"{$domowner}\"";

		$status = $this->execute($sql);
		if ($status != 0) {
			$this->rollbackTransaction();
			return -4;
		}

		return $this->endTransaction();
	}

	/**
	 * Drops a domain.
	 * @param $domain The name of the domain to drop
	 * @param $cascade True to cascade drop, false to restrict
	 * @return 0 success
	 */
	function dropDomain($domain, $cascade) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($domain);

		$sql = "DROP DOMAIN \"{$f_schema}\".\"{$domain}\"";
		if ($cascade) $sql .= " CASCADE";

		return $this->execute($sql);
	}

	/**
	 * Adds a check constraint to a domain
	 * @param $domain The domain to which to add the check
	 * @param $definition The definition of the check
	 * @param $name (optional) The name to give the check, otherwise default name is assigned
	 * @return 0 success
	 */
	function addDomainCheckConstraint($domain, $definition, $name = '') {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($domain);
		$this->fieldClean($name);

		$sql = "ALTER DOMAIN \"{$f_schema}\".\"{$domain}\" ADD ";
		if ($name != '') $sql .= "CONSTRAINT \"{$name}\" ";
		$sql .= "CHECK ({$definition})";

		return $this->execute($sql);
	}

	/**
	 * Drops a domain constraint
	 * @param $domain The domain from which to remove the constraint
	 * @param $constraint The constraint to remove
	 * @param $cascade True to cascade, false otherwise
	 * @return 0 success
	 */
	function dropDomainConstraint($domain, $constraint, $cascade) {
		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);
		$this->fieldClean($domain);
		$this->fieldClean($constraint);

		$sql = "ALTER DOMAIN \"{$f_schema}\".\"{$domain}\" DROP CONSTRAINT \"{$constraint}\"";
		if ($cascade) $sql .= " CASCADE";

		return $this->execute($sql);
	}

	// Function functions

	/**
	 * Returns all details for a particular function
	 * @param $func The name of the function to retrieve
	 * @return Function info
	 */
	function getFunction($function_oid) {
		$this->clean($function_oid);

		$sql = "
			SELECT
				pc.oid AS prooid, proname, 
				pg_catalog.pg_get_userbyid(proowner) AS proowner,
				nspname as proschema, lanname as prolanguage, procost, prorows,
				pg_catalog.format_type(prorettype, NULL) as proresult, prosrc,
				probin, proretset, proisstrict, provolatile, prosecdef,
				pg_catalog.oidvectortypes(pc.proargtypes) AS proarguments,
				proargnames AS proargnames,
				pg_catalog.obj_description(pc.oid, 'pg_proc') AS procomment,
				proconfig,
				(select array_agg( (select typname from pg_type pt
					where pt.oid = p.oid) ) from unnest(proallargtypes) p)
				AS proallarguments,
				proargmodes
			FROM
				pg_catalog.pg_proc pc, pg_catalog.pg_language pl,
				pg_catalog.pg_namespace pn
			WHERE
				pc.oid = '{$function_oid}'::oid AND pc.prolang = pl.oid
				AND pc.pronamespace = pn.oid
			";

		return $this->selectSet($sql);
	}

	/**
	 * Returns a list of all functions in the database
	 * @param $all If true, will find all available functions, if false just those in search path
	 * @param $type If not null, will find all functions with return value = type
	 *
  	 * @return All functions
	 */
	function getFunctions($all = false, $type = null) {
		if ($all) {
			$where = 'pg_catalog.pg_function_is_visible(p.oid)';
			$distinct = 'DISTINCT ON (p.proname)';

			if ($type) {
				$where .= " AND p.prorettype = (select oid from pg_catalog.pg_type p where p.typname = 'trigger') ";
			}
		}
		else {
			$c_schema = $this->_schema;
			$this->clean($c_schema);
			$where = "n.nspname = '{$c_schema}'";
			$distinct = '';
		}

		$sql = "
			SELECT
				{$distinct}
				p.oid AS prooid,
				p.proname,
				p.proretset,
				pg_catalog.format_type(p.prorettype, NULL) AS proresult,
				pg_catalog.oidvectortypes(p.proargtypes) AS proarguments,
				pl.lanname AS prolanguage,
				pg_catalog.obj_description(p.oid, 'pg_proc') AS procomment,
				p.proname || ' (' || pg_catalog.oidvectortypes(p.proargtypes) || ')' AS proproto,
				CASE WHEN p.proretset THEN 'setof ' ELSE '' END || pg_catalog.format_type(p.prorettype, NULL) AS proreturns,
				u.usename AS proowner
			FROM pg_catalog.pg_proc p
				INNER JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
				INNER JOIN pg_catalog.pg_language pl ON pl.oid = p.prolang
				LEFT JOIN pg_catalog.pg_user u ON u.usesysid = p.proowner
			WHERE NOT p.proisagg
				AND {$where}
			ORDER BY p.proname, proresult
			";

		return $this->selectSet($sql);
	}

	/**
	 * Returns an array containing a function's properties
	 * @param $f The array of data for the function
	 * @return An array containing the properties
	 */
	function getFunctionProperties($f) {
		$temp = array();

		// Volatility
		if ($f['provolatile'] == 'v')
			$temp[] = 'VOLATILE';
		elseif ($f['provolatile'] == 'i')
			$temp[] = 'IMMUTABLE';
		elseif ($f['provolatile'] == 's')
			$temp[] = 'STABLE';
		else
			return -1;

		// Null handling
		$f['proisstrict'] = $this->phpBool($f['proisstrict']);
		if ($f['proisstrict'])
			$temp[] = 'RETURNS NULL ON NULL INPUT';
		else
			$temp[] = 'CALLED ON NULL INPUT';

		// Security
		$f['prosecdef'] = $this->phpBool($f['prosecdef']);
		if ($f['prosecdef'])
			$temp[] = 'SECURITY DEFINER';
		else
			$temp[] = 'SECURITY INVOKER';

		return $temp;
	}

	/**
	 * Updates (replaces) a function.
	 * @param $function_oid The OID of the function
	 * @param $funcname The name of the function to create
	 * @param $newname The new name for the function
	 * @param $args The array of argument types
	 * @param $returns The return type
	 * @param $definition The definition for the new function
	 * @param $language The language the function is written for
	 * @param $flags An array of optional flags
	 * @param $setof True if returns a set, false otherwise
	 * @param $comment The comment on the function
	 * @return 0 success
	 * @return -1 transaction error
	 * @return -3 create function error
	 * @return -4 comment error
	 * @return -5 rename function error
	 * @return -6 alter owner error
	 * @return -7 alter schema error
	 */
	function setFunction($function_oid, $funcname, $newname, $args, $returns, $definition, $language, $flags, $setof, $funcown, $newown, $funcschema, $newschema, $cost, $rows, $comment) {
		// Begin a transaction
		$status = $this->beginTransaction();
		if ($status != 0) {
			$this->rollbackTransaction();
			return -1;
		}

		// Replace the existing function
		$status = $this->createFunction($funcname, $args, $returns, $definition, $language, $flags, $setof, $cost, $rows, $comment, true);
		if ($status != 0) {
			$this->rollbackTransaction();
			return $status;
		}

		$f_schema = $this->_schema;
		$this->fieldClean($f_schema);

		// Rename the function, if necessary
		$this->fieldClean($newname);
		/* $funcname is escaped in createFunction */
		if ($funcname != $newname) {
			$sql = "ALTER FUNCTION \"{$f_schema}\".\"{$funcname}\"({$args}) RENAME TO \"{$newname}\"";
			$status = $this->execute($sql);
			if ($status != 0) {
				$this->rollbackTransaction();
				return -5;
			}

            $funcname = $newname;
		}

		// Alter the owner, if necessary
		if ($this->hasFunctionAlterOwner()) {
			$this->fieldClean($newown);
		    if ($funcown != $newown) {
				$sql = "ALTER FUNCTION \"{$f_schema}\".\"{$funcname}\"({$args}) OWNER TO \"{$newown}\"";
				$status = $this->execute($sql);
				if ($status != 0) {
					$this->rollbackTransaction();
					return -6;
				}
		    }

		}

		// Alter the schema, if necessary
		if ($this->hasFunctionAlterSchema()) {
		    $this->fieldClean($newschema);
		    /* $funcschema is escaped in createFunction */
		    if ($funcschema != $newschema) { 
				$sql = "ALTER FUNCTION \"{$f_schema}\".\"{$funcname}\