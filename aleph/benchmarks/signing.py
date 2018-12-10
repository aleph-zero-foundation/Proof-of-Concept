import coincurve
import nacl.signing
from time import time

bench_size = 100000

print('bench size', bench_size)

messages = [str(i).encode() for i in range(bench_size)]

sk_cc = coincurve.PrivateKey()

print('benchmarking signing')

time_cc = time()
signs_cc = []
for msg in messages:
    signs_cc.append(sk_cc.sign(msg))

time_cc = round(time() - time_cc, 2)

sk_nc = nacl.signing.SigningKey.generate()

time_nc = time()
signs_nc = []
for msg in messages:
    signs_nc.append(sk_nc.sign(msg))

time_nc = round(time() - time_nc, 2)

print(f'coincurve {time_cc} nacl {time_nc}')


print('benchmarking verification')

vk_cc = coincurve.PublicKey.from_secret(sk_cc.secret)
signs_msg_cc = zip(signs_cc, messages)
time_cc = time()
for sig, msg in signs_msg_cc:
    vk_cc.verify(sig, msg)

time_cc = round(time() - time_cc, 2)

vk_nc = sk_nc.verify_key
signs_msg_nc = zip(signs_nc, messages)
time_nc = time()
for sig, msg in signs_msg_nc:
    vk_nc.verify(sig) == msg

time_nc = round(time() - time_nc, 2)

print(f'coincurve {time_cc} nacl {time_nc}')
