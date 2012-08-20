use 5.008004;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'DBI::Fetch',
    VERSION_FROM      => 'lib/DBI/Fetch.pm', # finds $VERSION
    PREREQ_PM         => { 
    	'Params::Callbacks' => 1.14 ,
    	'DBI'               => 1.5 , 
    	'Scalar::Util'      => 1.21 ,
    }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/DBI/Fetch.pm', # retrieve abstract from module
       AUTHOR         => 'Iain Campbell <cpanic@cpan.org>') : ()),
);
