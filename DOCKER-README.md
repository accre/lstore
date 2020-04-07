LStore Docker image building framework
=================================================

Structure
-------------------------------------------------
* lserver,lfs,depot  - Top level directories
  * files            - Everything under here is bundled into a tarball for the container
  * site             - This is also bundled into a tarball and is untarred AFTER all
                       "files" are untarred.  This has site specific information, for 
                       example configs, and mail configurations.
  * install          - A collection of scripts to run at the end of container creation.
                       The scripts are executed in alphabetical order.
  * docker           - Docker related bits, ie Dockerfile, etc
  * samples          - Sample files


LServer container
------------------------------------------------
Run `package-lserver.sh` to create a directory containing all the files needed
to create and deploy a new LServer container. This doesn't actually create a new
container it just localizes all the bits needed to create one into a single location
which can be copied to the destintaion server.

Running the command without options displays help information.

The LServer site specific information should contain the configs and nullmailer
setup information.  The post scripts setup sshd with a default password for login and
correctly set the timezone.

