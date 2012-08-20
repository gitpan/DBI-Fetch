# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl DBI-PAX.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;
use Test::More tests => 33;

BEGIN { use_ok('DBI::Fetch', ':all') }

is   DBI::Fetch::is_array_ref([]), 1, "is_array_ref";
isnt DBI::Fetch::is_array_ref({}), 1, "is_array_ref";
is   DBI::Fetch::is_hash_ref({}) , 1, "is_hash_ref";
isnt DBI::Fetch::is_hash_ref([]) , 1, "is_hash_ref";
is   DBI::Fetch::is_code_ref(sub {}) , 1, "is_code_ref";
isnt DBI::Fetch::is_code_ref([])     , 1, "is_code_ref";

our ($sql, $style, %style, @norm1, @norm2, @norm3);

$sql   = 'SELECT * FROM user WHERE User = ? AND Host = ?';
%style = DBI::Fetch::placeholder_disposition($sql);
$style = DBI::Fetch::placeholder_disposition($sql);
@norm1 = DBI::Fetch::normalize($sql, 'Fred', 'Bedrock');

is   $style{count},  2 , "placeholder_disposition correctly reports '?' placeholder count in list context";
is   $style{style}, '?', "placeholder_disposition correctly reports '?' placeholder style in list context";
is   $style       , '?', "placeholder_disposition correctly reports '?' placeholder style in scalar context";
ok   $norm1[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm1[1] eq 'Fred' &&
     $norm1[2] eq 'Bedrock', "'?' style statements normalize correctly";

$sql   = 'SELECT * FROM user WHERE User = :1 AND Host = :2';
%style = DBI::Fetch::placeholder_disposition($sql);
$style = DBI::Fetch::placeholder_disposition($sql);
@norm1 = DBI::Fetch::normalize($sql, 'Fred', 'Bedrock');
@norm2 = DBI::Fetch::normalize($sql, {':1' => 'Fred', ':2' => 'Bedrock'});
@norm3 = DBI::Fetch::normalize($sql, ['Fred', 'Bedrock']);

is   $style{count},  2  , "placeholder_disposition correctly reports ':1' placeholder count in list context";
is   $style{style}, ':1', "placeholder_disposition correctly reports ':1' placeholder style in list context";
is   $style       , ':1', "placeholder_disposition correctly reports ':1' placeholder style in scalar context";
ok   $norm1[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm1[1] eq 'Fred' &&
     $norm1[2] eq 'Bedrock', "':1' style statements normalize correctly";
ok   $norm2[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm2[1] eq 'Fred' &&
     $norm2[2] eq 'Bedrock', "':1' style statements normalize correctly using hash parameter list";
ok   $norm3[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm3[1] eq 'Fred' &&
     $norm3[2] eq 'Bedrock', "':1' style statements normalize correctly using array parameter list";

$sql   = 'SELECT * FROM user WHERE User = :user AND Host = :host';
%style = DBI::Fetch::placeholder_disposition($sql);
$style = DBI::Fetch::placeholder_disposition($sql);
@norm1 = DBI::Fetch::normalize($sql,  ':user' => 'Fred', ':host' => 'Bedrock' );
@norm2 = DBI::Fetch::normalize($sql, {':user' => 'Fred', ':host' => 'Bedrock'});
@norm3 = DBI::Fetch::normalize($sql, [':user' => 'Fred', ':host' => 'Bedrock']);

is   $style{count},  2     , "placeholder_disposition correctly reports ':name' placeholder count in list context";
is   $style{style}, ':name', "placeholder_disposition correctly reports ':name' placeholder style in list context";
is   $style       , ':name', "placeholder_disposition correctly reports ':name' placeholder style in scalar context";
ok   $norm1[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm1[1] eq 'Fred' &&
     $norm1[2] eq 'Bedrock', "':name' style statements normalize correctly";
ok   $norm2[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm2[1] eq 'Fred' &&
     $norm2[2] eq 'Bedrock', "':name' style statements normalize correctly using hash parameter list";
ok   $norm3[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm3[1] eq 'Fred' &&
     $norm3[2] eq 'Bedrock', "':name' style statements normalize correctly using array parameter list";

@norm1 = DBI::Fetch::normalize($sql,  user => 'Fred', host => 'Bedrock' );
@norm2 = DBI::Fetch::normalize($sql, {user => 'Fred', host => 'Bedrock'});
@norm3 = DBI::Fetch::normalize($sql, [user => 'Fred', host => 'Bedrock']);

ok   $norm1[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm1[1] eq 'Fred' &&
     $norm1[2] eq 'Bedrock', "':name' style statements normalize correctly (no colon on paramter names)";
ok   $norm2[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm2[1] eq 'Fred' &&
     $norm2[2] eq 'Bedrock', "':name' style statements normalize correctly using hash parameter list (no colon on paramter names)";
ok   $norm3[0] eq 'SELECT * FROM user WHERE User = ? AND Host = ?' &&
     $norm3[1] eq 'Fred' &&
     $norm3[2] eq 'Bedrock', "':name' style statements normalize correctly using array parameter list (no colon on paramter names)";

our $db_driver = $ENV{TEST_DB_DRIVER} || 'mysql';
our $db_name   = $ENV{TEST_DB_NAME}   || 'test';
our $db_host   = $ENV{TEST_DB_HOST}   || '127.0.0.1';
our $db_user   = $ENV{TEST_DB_USER}   || 'test';
our $db_pass   = $ENV{TEST_DB_PASS}   || 'test';
our $result;
our $dbh       = eval { 
    DBI->connect("dbi:$db_driver:database=$db_name;host=$db_host", $db_user, $db_pass, { RaiseError => 1, PrintError => 0, AutoCommit => 1})
};

eval { $dbh->do('DROP TABLE test') };

SKIP: {
    skip "Can't access a test database", 7 unless $dbh;

    ok DBI::Fetch::process($dbh, 'CREATE TABLE test ( id INT, name VARCHAR(255))') == 0, "process created table";

    my $sql = "INSERT INTO test (id, name) VALUES (1, 'Fred'), (2, 'Wilma'), (3, 'Barney'), (4, 'Betty')";

    ok DBI::Fetch::process($dbh, $sql) == 4, "process inserted 4 rows";

    my @result = DBI::Fetch::process($dbh, "SELECT * FROM test");

    ok @result == 4, "process returns 4 rows";

    my $callbacks;

    DBI::Fetch->push_config(return_result_sets_as_ref => 1, auto_pop_config => 1);

    $result = DBI::Fetch->process($dbh, "SELECT * FROM test WHERE name = :name", name => 'Fred', sub { $callbacks = 1; $_[0]{name} } );

    ok ref($result) && $result->[0] eq 'Fred', 'process returns result set as reference';

    @result = DBI::Fetch::process($dbh, "SELECT * FROM test WHERE name = :name", name => ['Fred', {}], sub { $callbacks = 1; $_[0]{name} } );

    ok @result == 1 && $result[0] eq 'Fred', "process returns 1 row";
    ok $callbacks == 1, "Callbacks work!";

    ok DBI::Fetch::process($dbh, 'DROP TABLE test') == 0, "process dropped table";
}
