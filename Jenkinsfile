#!/bin/groovy
String[] distros = ["centos-6", "centos-7",\
                    "debian-jessie", "ubuntu-trusty",\
                    "ubuntu-vivid", "ubuntu-wily"]

def compile_map = [:]

node('docker') {
    stage "Checkout"
    step([$class: 'GitHubCommitNotifier',
                resultOnFailure: 'FAILURE',
                statusMessage: [content: 'LStoreRoboto']])
    deleteDir()
    checkout scm
    sh "bash scripts/generate-docker-base.sh"
    sh "bash scripts/build-docker-base.sh ubuntu-xenial"
    zip archive: true, dir: '', glob: 'scripts/**', zipFile: 'scripts.zip'
    archive 'scripts/**'
    sh "bash scripts/check-patch.sh"
    stash includes: '**, .git/', name: 'source', useDefaultExcludes: false
    sh "env"
}
compile_map['unified'] = {
    node('xenial') {
        stage "Build-Unified"
        deleteDir()
        unstash 'source'
        dir('build') {
            sh "cmake -DBUILD_TESTS=on -DENABLE_COVERAGE=on -DENABLE_ASAN=on -DCMAKE_INSTALL_PREFIX=local/ .."
            sh "make -j8 externals"
            sh "make -j8 install 2>&1 | tee compile_log.txt"
            stash includes: 'local/**, run-tests, run-benchmarks', name: "unified-build"
            step([$class: 'WarningsPublisher', defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', parserConfigurations: [[parserName: 'GNU Make + GNU C Compiler (gcc)', pattern: 'compile_log.txt']], unHealthy: ''])
        }
    }

    node('xenial') {
        stage "UnitTests"
        deleteDir()
        unstash 'unified-build'
        sh "bash -c 'set -o pipefail ; LD_LIBRARY_PATH=local/lib UV_TAP_OUTPUT=1 ./run-tests 2>&1 | tee tap.log'"
        // step([$class: 'TapPublisher', testResults: 'tap.log'])
    }
}

compile_map['cppcheck'] = {
    node('xenial') {
        deleteDir()
        unstash 'source'
        dir('src') {
            sh "cppcheck --enable=all --inconclusive --xml --xml-version=2 \$(pwd) > cppcheck.xml"
            // step([$class: 'CppcheckPublisher'])
        }
    }
}

// cmake -DBUILD_SHARED_LIBS:BOOL=OFF -DINSTALL_TESTS:BOOL=ON ../source/

for (int i = 0 ; i < distros.size(); ++i) {
    def x = distros.get(i)
    compile_map["${x}"] = { node('docker') {
        deleteDir()
        unstash 'source'
        sh "bash scripts/generate-docker-base.sh ${x}"
        sh "bash scripts/build-docker-base.sh ${x}"
        sh "bash scripts/package.sh ${x}"
        sh "bash scripts/update-repo.sh ${x}"
        archive 'build/package/**'
        sh "bash scripts/test-repo.sh ${x}"
        stash includes: 'build/package/**', name: "${x}-package"
        dockerFingerprintFrom dockerfile: "scripts/docker/builder/${x}/Dockerfile", \
        image: "lstore/builder:${x}"
    } }
}


stage "Packaging"
parallel compile_map

node('docker') {
    stage "Deploying"
    deleteDir()
    unstash 'source'
    sh "bash scripts/generate-docker-base.sh"
    if (env.'JOB_NAME' == "LStore-Master") {
        env.'BRANCH_NAME' = 'master'
    }
    build job: 'LStore-Publish',
            parameters: [[$class: 'StringParameterValue',
                            name: 'upstream',
                            value: env.'BUILD_URL'], 
                         [$class: 'StringParameterValue',
                            name: 'branch',
                            value: env.'BRANCH_NAME']]
} 
