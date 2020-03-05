import { action, observable } from 'mobx';
import { apiCaller } from '@services/apiCaller';
import { IRootStore } from '.';

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
}

export class DestinationDefsListStore implements IDestinationDefsListStore {
  @observable public destinationDefs: IDestinationDef[] = [];
  @observable public rootStore: IRootStore;

  constructor(rootStore: IRootStore) {
    this.rootStore = rootStore;
  }

  @action.bound
  public async getDestinationDefs() {
    const res = await apiCaller().get(`/destination-definitions`);
    this.destinationDefs = res.data;
  }

  public getDestinationDef(id: string) {
    return this.destinationDefs.filter(
      (def: IDestinationDef) => def.id === id,
    )[0];
  }
}
