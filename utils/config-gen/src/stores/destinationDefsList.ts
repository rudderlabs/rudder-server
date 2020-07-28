import { action, observable } from 'mobx';
import { apiCaller } from '@services/apiCaller';
import { IRootStore } from '.';
import _ from 'lodash';

export interface IDestinationDefsListStore {
  destinationDefs: IDestinationDef[];
  rootStore: IRootStore;
  getDestinationDefs(): void;
  getDestinationDef(id: string): IDestinationDef;
}

export interface IDestinationDef {
  id: string;
  name: string;
  displayName: string;
  config: any;
}

export class DestinationDefsListStore implements IDestinationDefsListStore {
  @observable public destinationDefs: IDestinationDef[] = [];
  @observable public rootStore: IRootStore;

  constructor(rootStore: IRootStore) {
    this.rootStore = rootStore;
  }

  @action.bound
  public async getDestinationDefs() {
    const res = await apiCaller().get(`/open-destination-definitions`);
    this.destinationDefs = _.orderBy(
      res.data,
      [dest => dest.displayName.toString().toLowerCase()],
      ['asc'],
    );
  }

  public getDestinationDef(id: string) {
    return this.destinationDefs.filter(
      (def: IDestinationDef) => def.id === id,
    )[0];
  }
}
