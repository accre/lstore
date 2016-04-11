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
        dockerFingerprintFrom dockerfile: "scripts/docker/base/${x}/Dockerfile", \
        image: "lstore/builder:${x}"
    } }
}

stage "Checkout"
node {
    deleteDir()
    checkout scm
    sh "bash scripts/check-patch.sh"
    stash includes: '**, .git/', name: 'source', useDefaultExcludes: false
}

stage "Packaging"
parallel compile_map

