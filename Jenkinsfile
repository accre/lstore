String[] distros = ["centos-6", "centos-7",\
                    "debian-jessie", "ubuntu-trusty",\
                    "ubuntu-vivid", "ubuntu-wily"]

def compile_map = [:]

node('docker') {
    stage "Checkout"
    deleteDir()
    checkout scm
    sh "bash scripts/check-patch.sh"
    stash includes: '**, .git/', name: 'source', useDefaultExcludes: false
    stage "Update-Docker-Images"
    sh "bash scripts/build-docker-base.sh"
}
compile_map['unified'] = {
    node('xenial') {
        stage "Build-Unified"
        deleteDir()
        unstash 'source'
        dir('build') {
            sh "cmake -DBUILD_TESTS=on -DENABLE_COVERAGE=on -DENABLE_ASAN=on -DCMAKE_INSTALL_PREFIX=local/ .."
            sh "make -j8 install"
            stash includes: 'local/**, run-tests, run-benchmarks', name: "unified-build"
            step([$class: 'WarningsPublisher', canComputeNew: false, canResolveRelativePaths: false, consoleParsers: [[parserName: 'GNU Make + GNU C Compiler (gcc)']], defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', unHealthy: ''])
        }
    }

    node('xenial') {
        stage "UnitTests"
        deleteDir()
        unstash 'unified-build'
        sh "LD_LIBRARY_PATH=local/lib UV_TAP_OUTPUT=1 ./run-tests 2>&1 | tee tap.log"
        step([$class: 'TapPublisher', testResults: 'tap.log'])
    }
}

compile_map['cppcheck'] = {
    node('xenial') {
        deleteDir()
        unstash 'source'
        dir('src') {
            sh "cppcheck --enable=all --inconclusive --xml --xml-version=2 $(pwd) > cppcheck.xml"
            step([$class: 'CppcheckPublisher'])
        }
    }
}

for (int i = 0 ; i < distros.size(); ++i) {
    def x = distros.get(i)
    compile_map["${x}"] = { node('docker') {
        deleteDir()
        unstash 'source'
        sh "bash scripts/generate-docker-base.sh ${x}"
        sh "bash scripts/build-docker-base.sh ${x}"
        sh "bash scripts/package.sh ${x}"
        sh "bash scripts/update-repo.sh ${x}"
        sh "bash scripts/test-repo.sh ${x}"
        stash includes: 'build/repo/**', name: "${x}-repo"
        archive 'build/repo/**'
        dockerFingerprintFrom dockerfile: "scripts/docker/builder/${x}/Dockerfile", \
        image: "lstore/builder:${x}"
    } }
}


stage "Packaging"
parallel compile_map

