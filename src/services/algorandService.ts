import { IServiceOptions } from './IServiceOptions';
import AlgorandRepository from '../database/repositories/algorandRepository';

export default class AlgorandService {
  options: IServiceOptions;
  data;
  transaction;
  user;

  constructor(options) {
    this.options = options;
  }

  async getAlgoOverview(filter) {
    return AlgorandRepository.getOverview(
      this.options,
      filter,
    );
  }

  async putAlgoFavorite(assetId) {
    return AlgorandRepository.putFavorite(
      this.options,
      assetId,
    );
  }

  async putAlgoShowcase(assetId) {
    return AlgorandRepository.putShowcase(
      this.options,
      assetId,
    );
  }

  async getAlgoFavoriteList(filter) {
    return AlgorandRepository.getFavoriteList(
      this.options,
      filter,
    );
  }

  async getAlgoAssetList(filter) {
    return AlgorandRepository.getAssetList(
      this.options,
      filter,
    );
  }
  
  async getAlgoPoolList(filter) {
    return AlgorandRepository.getPoolList(
      this.options,
      filter,
    );
  }
  
  async getAlgoAsset(assetId, filter) {
    return AlgorandRepository.getAsset(
      this.options,
      assetId,
      filter,
    );
  }

  async getAlgoPool(address) {
    return AlgorandRepository.getPool(
      this.options,
      address,
    );
  }
}
