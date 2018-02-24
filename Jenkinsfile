#!/bin/groovy
String[] distros = ["centos-6", "centos-7"]

def compile_map = [:]

node('docker') {
    stage "Checkout"
    deleteDir()
    checkout scm
    sh '''bash scripts/generate-docker-base.sh
          bash scripts/build-docker-base.sh ubuntu-bionic
          bash scripts/check-patch.sh'''
    zip archive: true, dir: '', glob: 'scripts/**', zipFile: 'scripts.zip'
    archive 'scripts/**'
    stash includes: '**, .git/', name: 'source', useDefaultExcludes: false
    slackSend channel: 'jenkins', message: "Build Started - ${env.JOB_NAME} ${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)"
    sh "env"
}

compile_map['unified-gcc'] = {
    node('ubuntu-lts') {
        deleteDir()
        unstash 'source'
        dir('build') {
            try {
                sh '''cmake -DBUILD_TESTS=on -DENABLE_COVERAGE=on -DCMAKE_INSTALL_PREFIX=local/ ..
                    make -j8 externals
                    bash -c 'set -o pipefail ; make -j1 install VERBOSE=1 2>&1 | tee compile_log_gcc.txt'
                    bash -c 'set -o pipefail ; UV_TAP_OUTPUT=1 make coverage 2>&1 | tee unittest-output.txt' '''
            } catch (e) {
                def cores = findFiles(glob: 'core*')
                if (cores) {
                    zip archive: true, dir: '', glob: 'core*', zipFile: 'gcc-cores.zip'
                }
                throw e
            }
            stash includes: 'local/**, run-tests, run-benchmarks', name: "unified-gcc"
            stash includes: "compile_log_gcc.txt", name: "gcc-log"
            archive "coverage-html/**"
            publishHTML(target: [reportDir: 'coverage-html/', reportFiles: 'index.html', reportName: 'Test Coverage',keepAll: true])
            step([$class: "TapPublisher", testResults: "unittest-output.txt"])
        }
    }
}

compile_map['unified-clang'] = {
    node('ubuntu-lts') {
        deleteDir()
        unstash 'source'
        dir('build') {
            try {
                sh '''cmake -DBUILD_TESTS=on -DENABLE_COVERAGE=on -DCMAKE_INSTALL_PREFIX=local/ ..
                    make -j8 externals
                    bash -c 'set -o pipefail ; make -j1 install VERBOSE=1 2>&1 | tee compile_log_clang.txt' '''
            } catch (e) {
                // Ignore clang compilations for now
                def cores = findFiles(glob: 'core*')
                if (cores) {
                    zip archive: true, dir: '', glob: 'core*', zipFile: 'clang-cores.zip'
                }
            }
            stash includes: 'local/**, run-tests, run-benchmarks', name: "unified-clang"
            stash includes: "compile_log_clang.txt", name: "clang-log"
        }
    }
}

compile_map['tidy'] = {
    node('ubuntu-lts') {
        deleteDir()
        unstash "source"
        def tidy_checks="-*,misc-*,google-runtime-*,modernize-*,cert-*,performance-*,cppcoreguidelines-*,-misc-unused-parameters,readability-*,-readability-else-after-return,-readability-braces-around-statements" 
        dir('build') {
            sh "cmake  -DBUILD_APR=OFF -DENABLE_UPSTREAM_APR=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBUILD_TESTS=on -DCMAKE_INSTALL_PREFIX=local/ .."
            sh "clang-tidy -p=\$(pwd) '-header-filter=src/\\.*h\$' ../src/*/*.c ../src/*/*.h -checks=${tidy_checks} -list-checks | tee ../clang_tidy_log.txt"
            sh "clang-tidy -p=\$(pwd) '-header-filter=src/\\.*h\$' ../src/*/*.c ../src/*/*.h -checks=${tidy_checks} | tee -a ../clang_tidy_log.txt"
        }
        stash includes: "clang_tidy_log.txt", name: "clang-tidy-log"
    }
}

compile_map['scan-build'] = {
    node('ubuntu-lts') {
        deleteDir()
        unstash "source"
        def scan_checks = "-enable-checker alpha.core.BoolAssignment -enable-checker alpha.core.CallAndMessageUnInitRefArg -enable-checker alpha.core.CastSize -enable-checker alpha.core.CastToStruct -enable-checker alpha.core.DynamicTypeChecker -enable-checker alpha.core.FixedAddr -enable-checker alpha.core.IdenticalExpr -enable-checker alpha.core.PointerArithm -enable-checker alpha.core.PointerSub -enable-checker alpha.core.SizeofPtr -enable-checker alpha.core.TestAfterDivZero -enable-checker alpha.cplusplus.VirtualCall -enable-checker alpha.deadcode.UnreachableCode -enable-checker alpha.security.ArrayBound -enable-checker alpha.security.ArrayBoundV2 -enable-checker alpha.security.MallocOverflow -enable-checker alpha.security.ReturnPtrRange -enable-checker alpha.security.taint.TaintPropagation -enable-checker alpha.unix.Chroot -enable-checker alpha.unix.PthreadLock -enable-checker alpha.unix.SimpleStream -enable-checker alpha.unix.Stream -enable-checker alpha.unix.cstring.BufferOverlap -enable-checker alpha.unix.cstring.NotNullTerminated -enable-checker alpha.unix.cstring.OutOfBounds"
        dir('build') {
            sh """mkdir clang-static-analyzer
            env
            ls -lah /ccache
            ln -s \$(which ccache) clang
            PATH="\$(pwd):\$PATH"
            CCC_CC=\$(pwd)/clang scan-build --use-analyzer=\$(pwd)/clang cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=local/ -DBUILD_APR=OFF -DENABLE_UPSTREAM_APR=ON -DENABLE_CCACHE=OFF ..
            CCC_CC=\$(pwd)/clang scan-build --use-analyzer=\$(pwd)/clang -o clang-static-analyzer -v -v ${scan_checks} --keep-empty -maxloop 10 -stats make -j16 VERBOSE=1 || true
            mv clang-static-analyzer/* ../clang-report"""
        }
        archive "clang-report/**"
        publishHTML(target: [reportDir: 'clang-report/', reportFiles: 'index.html', reportName: 'Clang static analysis', keepAll: true])
    }
}

@NonCPS def loopArray(a) {
    a.collect{ v -> v }
}
for (def y in loopArray(distros)) {
    def x = y
    println "Processing outer ${x}"
    compile_map["${x}"] = { node('docker') {
        println "Processing inner ${x}"
        deleteDir()
        unstash 'source'
        sh """bash scripts/generate-docker-base.sh ${x}
              bash scripts/build-docker-base.sh ${x}
              bash scripts/package.sh ${x}"""
        sh """bash scripts/update-repo.sh ${x}
              bash scripts/test-repo.sh ${x}"""
        archive 'build/package/**'
        stash includes: 'build/package/**', name: "${x}-package"
        dockerFingerprintFrom dockerfile: "scripts/docker/builder/${x}/Dockerfile", \
                                image: "lstore/builder:${x}"
    } }
}

stage "Packaging"
parallel compile_map
node('ubuntu-lts') {
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
