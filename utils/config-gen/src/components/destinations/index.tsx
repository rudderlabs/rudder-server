import React, { Component } from 'react';
import { inject, observer } from 'mobx-react';

import { ISourcesListStore } from '@stores/sourcesList';
import { IDestinationsListStore } from '@stores/destinationsList';
import ConfiguredDestinations from '@components/configuredDestinations';
import { Container } from './styles';
import DestinationsCatalogue from '@components/destinationsCatalogue';

interface IDestinationsProps {
  history: any;
  sourcesListStore: ISourcesListStore;
  destinationsListStore: IDestinationsListStore;
}

@inject('sourcesListStore', 'destinationsListStore')
@observer
class Destinations extends Component<IDestinationsProps> {
  public render() {
    return (
      <Container className="Destinations">
        <DestinationsCatalogue />
        <ConfiguredDestinations />
      </Container>
    );
  }
}

export default Destinations;
