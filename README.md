# UDRAL PnP proof-of-concept

Proof-of-concept implementation of the automatic port-ID allocation for UDRAL.

This demo is based on PyUAVCAN; read the docs here: https://pyuavcan.readthedocs.io/en/stable/pages/demo.html

## Running

Before running the demo, you need to compile DSDL as follows:

```bash
yakut compile -O . https://github.com/UAVCAN/public_regulated_data_types/archive/master.zip \
                   https://github.com/Zubax/zubax_dsdl/archive/master.zip
```

When running this script for the first time, export the transport configuration via env vars.
If you are using UAVCAN/CAN via virtual SocketCAN:

```bash
export UAVCAN__CAN__IFACE="socketcan:vcan0"
export UAVCAN__NODE__ID=1
```

Afterward you can omit the environment variables because the configuration is stored in the "*.db" file.

Then run the script: `./udral_pnp.py`.
You can use these demos to test the script: https://github.com/UAVCAN/demos
