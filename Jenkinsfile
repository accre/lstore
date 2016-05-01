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
    parallel ["gcc-build" : {
        node('xenial') {
            stage "Build-Unified"
            deleteDir()
            unstash 'source'
            dir('build') {
                sh "cmake -DBUILD_TESTS=on -DENABLE_COVERAGE=on -DENABLE_ASAN=on -DCMAKE_INSTALL_PREFIX=local/ .."
                sh "make -j8 externals"
                sh "make -j1 install VERBOSE=1 2>&1 | tee compile_log_gcc.txt"
                stash includes: 'local/**, run-tests, run-benchmarks', name: "unified-build"
                stash includes: "compile_log_gcc.txt", name: "gcc-log"
            }
        }

        node('xenial') {
            stage "UnitTests"
            deleteDir()
            unstash 'unified-build'
            sh "bash -c 'set -o pipefail ; LD_LIBRARY_PATH=local/lib UV_TAP_OUTPUT=1 ./run-tests 2>&1 | tee tap.log'"
            // step([$class: 'TapPublisher', testResults: 'tap.log'])
        }},
        "clang-build" : {
            node('xenial') {
                stage "Build-Unified"
                deleteDir()
                unstash 'source'
                dir('build') {
                    sh "CC=clang cmake -DBUILD_TESTS=on -DENABLE_COVERAGE=on -DENABLE_ASAN=on -DCMAKE_INSTALL_PREFIX=local/ .."
                    sh "make -j8 externals"
                    sh "make -j1 install 2>&1 VERBOSE=1 | tee compile_log_clang.txt"
                    stash includes: "compile_log_clang.txt", name: "clang-log"
                }
            }}]
        node('xenial') {
            deleteDir()
            unstash "source"
            unstash "gcc-log"
            unstash "clang-log"
            dir('build') {
                sh "CC=clang cmake -DBUILD_TESTS=on -DENABLE_COVERAGE=on -DENABLE_ASAN=on -DCMAKE_INSTALL_PREFIX=local/ .."
                sh "clang-tidy -p=$(pwd) ../src/*/*.{c,h} -checks=misc-*,google-runtime-*,clang-analyzer-*,modernize-*,cert-*,performance-*,cppcoreguidelines-*,-misc-unused-parameters | tee ../clang_tidy_log.txt"
            }
            step([$class: 'WarningsPublisher', defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', parserConfigurations: [[parserName: 'GNU Make + GNU C Compiler (gcc)', pattern: '*.txt']], unHealthy: ''])
            step([$class: 'WarningsPublisher', defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', parserConfigurations: [[parserName: 'Clang (LLVM based)', pattern: 'compile_log_clang.txt']], unHealthy: ''])
            step([$class: 'WarningsPublisher', defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', parserConfigurations: [[parserName: 'GNU Make + GNU C Compiler (gcc)', pattern: 'clang_tidy_log.txt']], unHealthy: ''])
        }

}
//  CC=clang cmake ../source/ -DCMAKE_INSTALL_PREFIX=local/ -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

// scan-build cmake ../source/ -DCMAKE_INSTALL_PREFIX=local/ -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
// scan-build -o clangScanBuildReports -v -v make VERBOSE=1 clean all
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
