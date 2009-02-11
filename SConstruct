# -*- python -*-
#
# Setup our environment
#
import glob, os.path, sys
import lsst.SConsUtils as scons

env = scons.makeEnv(
    "meas_pipeline",
    r"$HeadURL$",
    [
        ["boost", "boost/version.hpp", "boost_filesystem:C++"],
        ["boost", "boost/regex.hpp", "boost_regex:C++"],                    
        ["boost", "boost/serialization/serialization.hpp", "boost_serialization:C++"],
        ["boost", "boost/serialization/base_object.hpp", "boost_serialization:C++"],
        ["boost", "boost/test/unit_test.hpp", "boost_unit_test_framework:C++"],
        ["python", "Python.h"],
        
        ["utils", "lsst/utils/Utils.h", "utils:C++"],
        ["pex_exceptions", "lsst/pex/exceptions.h","pex_exceptions:C++"],
        ["daf_base", "lsst/daf/base/Citizen.h", "pex_exceptions daf_base:C++"],
        ["pex_logging", "lsst/pex/logging/Component.h", "pex_logging:C++"],
        ["pex_policy", "lsst/pex/policy/Policy.h","pex_policy:C++"],
        ["daf_persistence", "lsst/daf/persistence.h", "daf_persistence:C++"], 
    ])

env.libs["meas_pipeline"] += env.getlibs("boost utils daf_base daf_persistence pex_exceptions pex_logging pex_policy")

for d in (".", "doc", "examples", "lib", "python/lsst/meas/pipeline", "tests"):
    if d != ".":
        try:
            SConscript(os.path.join(d, "SConscript"))
        except Exception, e:
            print >> sys.stderr, "%s: %s" % (os.path.join(d, "SConscript"), e)
    Clean(d, Glob(os.path.join(d, "*~")))
    Clean(d, Glob(os.path.join(d, "*.pyc")))


env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"

Alias("install", [
    env.Install(env['prefix'], "python"),
    env.Install(env['prefix'], "include"),
    env.Install(env['prefix'], "lib"),
    env.InstallAs(os.path.join(env['prefix'], "doc", "doxygen"), os.path.join("doc", "htmlDir")),
    env.InstallEups(os.path.join(env['prefix'], "ups"), glob.glob(os.path.join("ups", "*.table")))
])

scons.CleanTree(r"*~ core *.so *.os *.o")

#
# Build TAGS files
#
files = scons.filesToTag()
if files:
    env.Command("TAGS", files, "etags -o $TARGET $SOURCES")



env.Declare()
env.Help("""
LSST Measurement Pipeline Packages
""")
    
