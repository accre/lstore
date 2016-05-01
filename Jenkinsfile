#!/bin/groovy
String[] distros = ["centos-6", "centos-7",
                    "debian-jessie", "ubuntu-trusty",
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
compile_map['unified-gcc'] = {
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
    }
}

compile_map['unified-clang'] = {
    node('xenial') {
        deleteDir()
        unstash 'source'
        dir('build') {
            sh "CC=clang cmake -DBUILD_TESTS=on -DENABLE_COVERAGE=on -DENABLE_ASAN=on -DCMAKE_INSTALL_PREFIX=local/ .."
            sh "make -j8 externals"
            sh "make -j1 install 2>&1 VERBOSE=1 | tee compile_log_clang.txt"
            stash includes: "compile_log_clang.txt", name: "clang-log"
        }
    }
    node('xenial') {
        deleteDir()
        unstash 'unified-build'
        sh "bash -c 'set -o pipefail ; LD_LIBRARY_PATH=local/lib UV_TAP_OUTPUT=1 ./run-tests 2>&1 | tee tap.log'"
        // step([$class: 'TapPublisher', testResults: 'tap.log'])
    }
}


compile_map['tidy'] = {
    node('xenial') {
        deleteDir()
        unstash "source"
        dir('build') {
            sh "CC=clang cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBUILD_TESTS=on -DCMAKE_INSTALL_PREFIX=local/ .."
            sh "make externals"
            sh "clang-tidy -p=\$(pwd) '-header-filter=../src/*.h' ../src/*/*.c ../src/*/*.h -checks=-*,misc-*,google-runtime-*,modernize-*,cert-*,performance-*,cppcoreguidelines-*,-misc-unused-parameters | tee ../clang_tidy_log.txt"
        }
        stash includes: "clang_tidy_log.txt", name: "clang-tidy-log"
    }
}

compile_map['scan-build'] = {
    node('xenial') {
        deleteDir()
        unstash "source"
        def scan_checks = "-enable-checker alpha.core.BoolAssignment -enable-checker alpha.core.CallAndMessageUnInitRefArg -enable-checker alpha.core.CastSize -enable-checker alpha.core.CastToStruct -enable-checker alpha.core.DynamicTypeChecker -enable-checker alpha.core.FixedAddr -enable-checker alpha.core.IdenticalExpr -enable-checker alpha.core.PointerArithm -enable-checker alpha.core.PointerSub -enable-checker alpha.core.SizeofPtr -enable-checker alpha.core.TestAfterDivZero -enable-checker alpha.cplusplus.VirtualCall -enable-checker alpha.deadcode.UnreachableCode -enable-checker alpha.security.ArrayBound -enable-checker alpha.security.ArrayBoundV2 -enable-checker alpha.security.MallocOverflow -enable-checker alpha.security.ReturnPtrRange -enable-checker alpha.security.taint.TaintPropagation -enable-checker alpha.unix.Chroot -enable-checker alpha.unix.PthreadLock -enable-checker alpha.unix.SimpleStream -enable-checker alpha.unix.Stream -enable-checker alpha.unix.cstring.BufferOverlap -enable-checker alpha.unix.cstring.NotNullTerminated -enable-checker alpha.unix.cstring.OutOfBounds"
        dir('build') {
            sh "CCC_CC=clang scan-build cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBUILD_TESTS=on -DCMAKE_INSTALL_PREFIX=local/ .."
            sh "CC=clang make externals"
            sh "CCC_CC=clang scan-build -o clang-static-analyzer -v -v ${scan_checks} --keep-empty make -j4"
            sh "mv clang-static-analyzer/* ../clang-report"
        }
        archive "clang-report/**"
        publishHTML(target: [reportDir: 'clang-report/', reportFiles: 'index.html', reportName: 'Clang'])
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
node('xenial') {
    deleteDir()
    unstash "source"
    unstash "gcc-log"
    unstash "clang-log"
    unstash "clang-tidy-log"
    step([$class: 'WarningsPublisher', defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', parserConfigurations: [[parserName: 'GNU Make + GNU C Compiler (gcc)', pattern: '*.txt']], unHealthy: ''])
}


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
