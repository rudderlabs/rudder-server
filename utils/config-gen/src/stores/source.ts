import { ISourceDefintion } from '@stores/sourceDefinitionsList';
import { action, computed, observable, reaction, trace } from 'mobx';

import { IDestinationStore } from './destination';
import { IRootStore } from './index';

export interface ISourceStore {
  id: string;
  name: string;
  writeKey: string;
  enabled: boolean;
  config: JSON;
  sourceDefinitionId: any;
  sourceDef: ISourceDefintion;
  rootStore: IRootStore;
  destinations: IDestinationStore[];
  setName(name: string): void;
  toggleEnabled(): void;
}

export class SourceStore implements ISourceStore {
  @observable public id: string;
  @observable public name: string;
  @observable public writeKey: string;
  @observable public enabled: boolean;
  @observable public config: JSON;
  @observable public sourceDefinitionId: any;
  @observable public rootStore: IRootStore;

  constructor(source: ISourceStore, rootStore: IRootStore) {
    this.id = source.id;
    this.name = source.name;
    this.writeKey = source.writeKey;
    this.enabled = source.enabled;
    this.config = source.config;
    this.sourceDefinitionId = source.sourceDefinitionId;
    this.rootStore = rootStore;
  }

  @action.bound
  public setName(name: string): void {
    this.name = name;
  }

  @computed get sourceDef() {
    return this.rootStore.sourceDefinitionsListStore.getSourceDef(
      this.sourceDefinitionId,
    );
  }

  @computed get destinations() {
    let destIds = this.rootStore.connectionsStore.connections[this.id] || [];
    return this.rootStore.destinationsListStore.destinations.filter(dest => {
      return destIds.indexOf(dest.id) > -1;
    });
  }

  // useful for debugging
  /* reaction = reaction(
    () => this.rootStore.destinationsListStore.destinations,
    dests => {
      console.log('changing...', dests);
    },
  ); */

  @action.bound
  /**
   * toggleEnabled
   */
  public async toggleEnabled() {
    this.enabled = !this.enabled;
  }
}
