import styled from 'styled-components';
import is from 'typescript-styled-is';

interface IProps {
  wrap?: string;
  spaceBetween?: boolean;
  justifyContentCenter?: boolean;
  flexDirection?: string;
  alignItems?: string;
  margin?: string;
}

export const Flex = styled.div<IProps>`
  display: flex;

  ${is('margin')`
    margin: ${props => props.margin}
  `};

  ${is('flexDirection')`
    flex-direction: ${props => props.flexDirection};
  `};

  ${is('alignItems')`
    align-items: ${props => props.alignItems};
  `};

  ${is('wrap')`
    flex-wrap: wrap;
  `};

  ${is('spaceBetween')`
    justify-content: space-between;
  `};

  ${is('justifyContentCenter')`
    justify-content: center;
  `};
`;
