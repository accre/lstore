String[] distros = ["centos-6", "centos-7",\
                    "debian-jessie", "ubuntu-trusty",\
                    "ubuntu-vivid", "ubuntu-wily"]

def compile_map = [:]
for (int i = 0 ; i < distros.size(); ++i) {
    def x = distros.get(i)
    echo "Examining ${x}"
    compile_map["${x}"] = { node('docker') {
        deleteDir()
        echo "Executing ${x}"
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

node('docker') {
    stage "Checkout"
    deleteDir()
    checkout scm
    sh "bash -x scripts/check-patch.sh"
    stash includes: '**, .git/', name: 'source', useDefaultExcludes: false
    stage "Update-Docker-Images"
    sh "bash scripts/build-docker-base.sh"
}
node('xenial') {
    stage "Build-Unified"
    deleteDir()
    unstash 'source'
    dir('build') {
        sh "cmake -DBUILD_TESTS=on -DENABLE_COVERAGE=on -DCMAKE_INSTALL_PREFIX=local/ .."
        sh "make -j8 install"
        stash includes: 'local/**, run-tests, run-benchmarks', name: "unified-build"
    }
}
node('xenial') {
    stage "UnitTests"
    deleteDir()
    unstash 'unified-build'
    sh "LD_LIBRARY_PATH=local/lib UV_TAP_OUTPUT=1 ./run-tests"
}
stage "Packaging"
parallel compile_map

