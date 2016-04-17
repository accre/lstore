Descriptor of docker configs - docker/builder
==========================================

Adding another supported distribution is simple - add a new directory under
this directory, with a .gitignore to keep git from saving the dockerfile. Once
that's done, modify generate-docker-base.sh to get dockerfile to do the right
thing to build a base-image for your new target.
