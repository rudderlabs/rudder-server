import React, { Component } from 'react';
import { observer } from 'mobx-react';

import ConfiguredSources from '@components/configuredSources';
import { Container } from './styles';
import SourcesCatalogue from '@components/sourcesCatalogue';

interface ISourcesProps {}

@observer
class Sources extends Component<ISourcesProps> {
  public render() {
    return (
      <Container className="Sources">
        <SourcesCatalogue />
        <ConfiguredSources />
      </Container>
    );
  }
}

export default Sources;
