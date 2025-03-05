# Purpose

These files/folders are an attempt to improve on our current docker 'templates'

The docker image resulting from [the driver Hail Dockerfile](../driver/Dockerfile.hail) contains a variety of libraries,
designed to suit any potential Analysis-Runner job. This includes a GCloud installation, and a Hail install built from
source. It also includes:

* cpg-workflows (pipeline)
* analysis-runner
* sample-metadata (deprecated)
* metamist (metadata database interface)
* gcsfs (python/GCS interface)

These libraries are not pinned during the installation, so each run of the driver building workflow may result in a 
different collection of versions. This is not ideal, as it may lead to unexpected behaviour in the future.

It also leaves the 'Driver' image acting less as a template to build on, and more as a full-featured execution
environment. This image decompressed is **9.53GB**. In spite of this, we build on top of this in [several places](https://github.com/search?q=%22FROM+australia-southeast1-docker.pkg.dev%2Fanalysis-runner%2Fimages%2Fdriver%3Alatest%22&type=code):

* cpg_workflows (every pipeline image)
* cpg_flow (new workflow framework)
* cpg-infrastructure (storage-vis)
* cpg-utils (image not used AFAIK)

The aim with these build files is to re-think the template use-case, and design a fresh round of images which can be 
used for templating. Main goals:

* lightweight (e.g. no GCloud, unless required)
* minimal (let each consumer build the way they need to, without being prescriptive)
* flexible (e.g. different python versions)

I've gotten some way towards lightweight (for a given value of light...). The previous driver candidate was 9.53GB. With
these builds that's now down to 4.25GB with GCloud, or 3.23GB without, saving 5.28GB or 6.3GB respectively. The publicly
available HailGenetics image is 3.19GB at the same Hail version, so we're not far off that.

These are definitely closer to minimal, not installing any CPG libraries. 

There's the potential for flexibility on python version, but it's not well characterised, and downstream of this we haven't 
assessed the impact on the images that build on top of these.
