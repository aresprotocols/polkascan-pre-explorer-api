# inflation_params
auction_adjust = 0
auction_max = 0
num_auctions = 0
falloff = 0.05
max_inflation = 0.1
min_inflation = 0.025
stake_target = 0.50

# total_issuance = 1004912918475693903101
# total_stake = 25463166148786513842

total_issuance = 1005143929452533053703
total_stake = 31506535424833259436

# totalStaked = 31506535424833259436, totalIssuance = 1005143929452533053703

staked_fraction = 0 if total_issuance == 0 or total_issuance == 0 else total_stake * 1000000 / total_issuance / 1000000
print('staked_fraction = ', staked_fraction)
# staked_Fraction = total_issuance == 0 || totalIssuance.isZero()? 0 : totalStaked.mul(BN_MILLION).div(totalIssuance).toNumber() / BN_MILLION.toNumber();
ideal_stake = stake_target - (min(auction_max, num_auctions) * auction_adjust)
print('ideal_stake = ', ideal_stake)
ideal_interest = max_inflation / ideal_stake
print('ideal_interest = ', ideal_interest)
if staked_fraction <= ideal_stake:
    tmp = staked_fraction * (ideal_interest - (min_inflation / ideal_stake))
else:
    tmp = (ideal_interest * ideal_stake - min_inflation) * (2 ** ((ideal_stake - staked_fraction) / falloff))
inflation = 100 * (min_inflation + tmp)

print(inflation)