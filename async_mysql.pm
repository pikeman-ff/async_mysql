package MySQLDBI;
use EV;
use DBI;
use base 'Exporter';
use strict;
our @EXPORT    = qw(create_dbh_pool get_dbh put_dbh dbh_exec);

my $dbh_pool = [];
my ($db_name,$db_host,$db_user,$db_pass) ;

#create dbh pool
sub create_dbh_pool
{
    ($db_name,$db_host,$db_user,$db_pass,$size) = @_;
    for ( 1 .. $size ) {
         my $dbh = DBI->connect(
             "dbi:mysql:database=$db_name;host=$db_host",$db_user,$db_pass,
             {mysql_enable_utf8 => 1,'RaiseError'=>1},
         );
         $dbh->do('set SESSION wait_timeout=72000');
         $dbh->do('set SESSION interactive_timeout=72000');

        push(@{$dbh_pool},
                {
                    handle => $dbh,
                }
            );
    }
}

#get dbh handle from pool
sub get_dbh
{
    return pop @{$dbh_pool} if scalar(@{$dbh_pool});
    my $dbh = DBI->connect(
             "dbi:mysql:database=$db_name;host=$db_host",$db_user,$db_pass,
             {mysql_enable_utf8 => 1,'RaiseError'=>1},
         );
    $dbh->do('set SESSION wait_timeout=72000');
    $dbh->do('set SESSION interactive_timeout=72000');

    return
        {
            handle => $dbh,
        };
}

#put dbh handle back to pool
sub put_dbh
{
    push(@{$dbh_pool},shift);
}

#exec sql statement. when mysql has result back,call callback func.
sub dbh_exec
{
    my ($st,$args,$cb) = @_;

    my $dbh = get_dbh();
    return $cb->(undef,undef) unless $dbh ;

    my $sth = undef;
    $sth = $dbh->{handle}->prepare($st,{async =>1});
    if ( $args ) {
        eval {$sth->execute(@{$args})};
    } else {
        eval { $sth->execute() }
    }
    my $w;
    if ( $@ =~ /gone/i ) {
        undef $dbh;

        $dbh = DBI->connect(
             "dbi:mysql:database=$db_name;host=$db_host","$db_user","$db_pass",
             {mysql_enable_utf8 => 1,'RaiseError'=>1}
         ) or die "can not connect to db!\n";
         $dbh->do('set SESSION wait_timeout=72000');
         $dbh->do('set SESSION interactive_timeout=72000');

        $sth = $dbh->prepare($st,{async=>1});
        $w = EV::io $dbh->mysql_fd,EV::READ,sub{
            $cb->($dbh,$sth);
            delete $dbh->{w};
            put_dbh({ handle => $dbh});
        };
        if ( $args ) {
            eval {$sth->execute(@{$args})};
        } else {
            eval { $sth->execute() }
        }
    }
    $w = EV::io $dbh->{handle}->mysql_fd,EV::READ,sub{
         my $w=shift;
         $cb->($dbh,$sth);
         delete $dbh->{w};
         put_dbh($dbh);
    };
    $dbh->{w} = $w;
}
1;
__END__

=pod
=head1 NAME

MySQLDBI - a MySQL async caller DBI

=head1 SYNOPSIS

use MySQLDBI;
create_dbh(...);
$dbh = get_dbh();
$dbh->dbh_exec("SQL statement",$args_array_ref,sub {
    my ($dbh,$sth) = @_;
    #...
});
